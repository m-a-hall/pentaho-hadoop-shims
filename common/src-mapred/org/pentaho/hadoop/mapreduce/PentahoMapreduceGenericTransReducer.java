/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.hadoop.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceHelper.MROperations;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceHelper.PentahoMapreduceOutputCollectorRowListener;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

public class PentahoMapreduceGenericTransReducer<K, V, K2, V2> extends Reducer<K, V, K2, V2> {

  protected PentahoMapreduceHelper<K2, V2> m_helper = new PentahoMapreduceHelper<K2, V2>();

  protected RowProducer rowProducer;
  protected Object value;
  protected InKeyValueOrdinals inOrdinals = null;
  protected TypeConverterFactory typeConverterFactory;
  protected ITypeConverter inConverterK = null;
  protected ITypeConverter inConverterV = null;
  protected RowMetaInterface injectorRowMeta;
  protected SingleThreadedTransExecutor executor;

  public PentahoMapreduceGenericTransReducer() throws KettleException {
    super();

    this.setMRType( MROperations.Reduce );
    typeConverterFactory = new TypeConverterFactory();
  }

  @Override
  public void setup( Context context ) throws IOException {
    m_helper.setup( context );
  }

  @Override
  public void reduce( K key, Iterable<V> values, Context context ) throws IOException {
    try {
      PentahoMapreduceHelper.setDebugStatus( context, "Begin processing record", m_helper.debug );

      // Just to make sure the configuration is not broken...
      //
      if ( m_helper.trans == null ) {
        throw new RuntimeException( "Error initializing transformation.  See error log." ); //$NON-NLS-1$
      }

      if ( isSingleThreaded() ) {

        // Only ever initialize once!
        //
        if ( !m_helper.trans.isRunning() ) {

          // The transformation needs to be prepared and started...
          //
          m_helper.shareVariableSpaceWithTrans( context );
          m_helper.setTransLogLevel( context );
          prepareExecution( context );
          addInjectorAndProducerToTrans( key, values, context, getInputStepName(), getOutputStepName() );

          // Create the Single Threader executor, sort the steps, and so on...
          //
          executor = new SingleThreadedTransExecutor( m_helper.trans );

          // This validates whether or not a step is capable of running in Single Threaded mode.
          //
          boolean ok = executor.init();
          if ( !ok ) {
            throw new KettleException(
                "Unable to initialize the single threaded transformation, check the log for details." );
          }

          // The transformation is considered in a "running" state now.
        }

        // The following 2 statements are the only things left to do for one set of data coming from Hadoop...
        //

        // Inject the values, including the one we probed...
        //
        injectValues( key, values, context );

        // Signal to the executor that we have enough data in the pipeline to do one iteration.
        // All steps are executed in a loop once in sequence, one after the other.
        //
        executor.oneIteration();
      } else {
        // Clean up old logging
        //
        KettleLogStore.discardLines( m_helper.trans.getLogChannelId(), true );

        // Create a copy of trans so we don't continue to add new TransListeners and run into a
        // ConcurrentModificationException
        // when this reducer is reused "quickly"

        m_helper.trans = MRUtil.recreateTrans( m_helper.trans );

        m_helper.shareVariableSpaceWithTrans( context );
        m_helper.setTransLogLevel( context );
        prepareExecution( context );
        addInjectorAndProducerToTrans( key, values, context, getInputStepName(), getOutputStepName() );

        try {
          // Inject the values, including the one we probed...
          //
          injectValues( key, values, context );
          m_helper.trans.waitUntilFinished();
          PentahoMapreduceHelper.setDebugStatus( context, "Transformation has finished", m_helper.debug );
        } finally {
          disposeTransformation();
        }
      }

      if ( m_helper.trans.getErrors() > 0 ) {
        PentahoMapreduceHelper.setDebugStatus( context, "Errors detected in reducer/combiner transformation",
            m_helper.debug );

        List<KettleLoggingEvent> logList =
            KettleLogStore.getLogBufferFromTo( m_helper.trans.getLogChannelId(), false, 0, KettleLogStore
                .getLastBufferLineNr() );

        StringBuffer buff = new StringBuffer();
        for ( KettleLoggingEvent le : logList ) {
          if ( le.getLevel() == LogLevel.ERROR ) {
            buff.append( le.getMessage().toString() ).append( "\n" );
          }
        }
        throw new Exception( "Errors were detected for reducer/combiner transformation:\n\n" + buff.toString() );
      }

    } catch ( Exception e ) {
      printException( context, e );
      PentahoMapreduceHelper.setDebugStatus( context, "An exception was raised", m_helper.debug );
      throw new IOException( e );
    }
  }

  public void setMRType( MROperations mrOperation ) {
    m_helper.setMRType( mrOperation );
  }

  public boolean isSingleThreaded() {
    return m_helper.reduceSingleThreaded;
  }

  public String getInputStepName() {
    return m_helper.reduceInputStepName;
  }

  public String getOutputStepName() {
    return m_helper.reduceOutputStepName;
  }

  private void printException( Context context, Exception e ) throws IOException {
    e.printStackTrace( System.err );
    PentahoMapreduceHelper.setDebugStatus( context, "An exception was raised", m_helper.debug );
    throw new IOException( e );
  }

  private void disposeTransformation() {
    try {
      m_helper.trans.stopAll();
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
    try {
      m_helper.trans.cleanup();
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
  }

  private void injectValues( final K key, final Iterable<V> values, Context context ) throws Exception {
    if ( rowProducer != null ) {
      // Execute row injection
      // We loop through the values to do this

      if ( value != null ) {
        if ( inOrdinals != null ) {
          m_helper.injectValue( key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals.getValueOrdinal(),
              inConverterV, injectorRowMeta, rowProducer, context );
        } else {
          m_helper.injectValue( key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, context );
        }
      }

      for ( V vl : values ) {
        value = vl;

        if ( inOrdinals != null ) {
          m_helper.injectValue( key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals.getValueOrdinal(),
              inConverterV, injectorRowMeta, rowProducer, context );
        } else {
          m_helper.injectValue( key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, context );
        }
      }

      // make sure we don't pick up a bogus row next time this method is called without rows.
      //
      value = null;

      rowProducer.finished();
    }
  }

  private void prepareExecution( Context context ) throws KettleException {
    PentahoMapreduceHelper.setDebugStatus( context, "Preparing transformation for execution", m_helper.debug );
    m_helper.trans.prepareExecution( null );
  }

  private void addInjectorAndProducerToTrans( K key, Iterable<V> values, Context context, String inputStepName,
      String outputStepName ) throws Exception {
    PentahoMapreduceHelper.setDebugStatus( context, "Locating output step: " + outputStepName, m_helper.debug );
    StepInterface outputStep = m_helper.trans.findRunThread( outputStepName );
    if ( outputStep != null ) {
      m_helper.rowCollector =
          new PentahoMapreduceOutputCollectorRowListener<K2, V2>( context, m_helper.outClassK, m_helper.outClassV,
              m_helper.debug );
      outputStep.addRowListener( m_helper.rowCollector );

      injectorRowMeta = new RowMeta();
      PentahoMapreduceHelper.setDebugStatus( context, "Locating input step: " + inputStepName, m_helper.debug );
      if ( inputStepName != null ) {
        // Setup row injection
        rowProducer = m_helper.trans.addRowProducer( inputStepName, 0 );
        StepInterface inputStep = rowProducer.getStepInterface();
        StepMetaInterface inputStepMeta = inputStep.getStepMeta().getStepMetaInterface();

        inOrdinals = null;
        if ( inputStepMeta instanceof BaseStepMeta ) {
          PentahoMapreduceHelper.setDebugStatus( context,
              "Generating converters from RowMeta for injection into the transformation", m_helper.debug );

          // Convert to BaseStepMeta and use getFields(...) to get the row meta and therefore the expected input types
          ( (BaseStepMeta) inputStepMeta ).getFields( injectorRowMeta, null, null, null, null );

          inOrdinals = new InKeyValueOrdinals( injectorRowMeta );

          if ( inOrdinals.getKeyOrdinal() < 0 || inOrdinals.getValueOrdinal() < 0 ) {
            throw new KettleException( "key or value is not defined in transformation injector step" );
          }

          // Get a converter for the Key if the value meta has a concrete Java class we can use.
          // If no converter can be found here we wont do any type conversion.
          if ( injectorRowMeta.getValueMeta( inOrdinals.getKeyOrdinal() ) != null ) {
            inConverterK =
                typeConverterFactory.getConverter( key.getClass(), injectorRowMeta.getValueMeta( inOrdinals
                    .getKeyOrdinal() ) );
          }

          // we need to peek into the first value to get the class (the combination of Iterator and generic makes this a
          // pain)

          if ( values.iterator().hasNext() ) {
            value = values.iterator().next();
          }
          if ( value != null ) {
            // Get a converter for the Value if the value meta has a concrete Java class we can use.
            // If no converter can be found here we wont do any type conversion.
            if ( injectorRowMeta.getValueMeta( inOrdinals.getValueOrdinal() ) != null ) {
              inConverterV =
                  typeConverterFactory.getConverter( value.getClass(), injectorRowMeta.getValueMeta( inOrdinals
                      .getValueOrdinal() ) );
            }
          }
        }

        m_helper.trans.startThreads();
      } else {
        PentahoMapreduceHelper.setDebugStatus( context, "No input stepname was defined", m_helper.debug );
      }

      if ( m_helper.getException() != null ) {
        PentahoMapreduceHelper.setDebugStatus( context, "An exception was generated by the transformation",
            m_helper.debug );
        // Bubble the exception from within Kettle to Hadoop
        throw m_helper.getException();
      }

    } else {
      if ( outputStepName != null ) {
        PentahoMapreduceHelper.setDebugStatus( context, "Output step [" + outputStepName + "] could not be found",
            m_helper.debug );
        throw new KettleException( "Output step not defined in transformation" );
      } else {
        PentahoMapreduceHelper.setDebugStatus( context, "Output step name not specified", m_helper.debug );
      }
    }
  }

  @Override
  public void cleanup( Context context ) throws IOException, InterruptedException {
    // Stop the executor if any is defined...
    if ( isSingleThreaded() && executor != null ) {
      try {
        executor.dispose();
      } catch ( KettleException e ) {
        e.printStackTrace( System.err );
        m_helper.trans.getLogChannel().logError( "Error disposing of single threading transformation: ", e );
      }

      // Clean up old log lines
      //
      KettleLogStore.discardLines( m_helper.trans.getLogChannelId(), true );

    }

    super.cleanup( context );
  }
}
