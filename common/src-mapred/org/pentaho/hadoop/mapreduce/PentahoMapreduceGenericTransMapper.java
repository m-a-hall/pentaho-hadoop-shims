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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceHelper.MROperations;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceHelper.PentahoMapreduceOutputCollectorRowListener;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

public class PentahoMapreduceGenericTransMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {

  protected PentahoMapreduceHelper<K2, V2> m_helper = new PentahoMapreduceHelper<K2, V2>();
  protected TypeConverterFactory typeConverterFactory;

  // protected PentahoMapreduceOutputCollectorRowListener<K2, V2> rowCollector;

  public PentahoMapreduceGenericTransMapper() throws KettleException {
    super();

    m_helper.setMRType( MROperations.Map );
    typeConverterFactory = new TypeConverterFactory();
  }

  @Override
  public void setup( Context context ) throws IOException {
    m_helper.setup( context );

    // Pass some information to the transformation...
    //
    Configuration conf = context.getConfiguration();
    m_helper.variableSpace.setVariable( "Internal.Hadoop.NumMapTasks", conf.get( "mapred.map.tasks", "1" ) );
    m_helper.variableSpace.setVariable( "Internal.Hadoop.NumReduceTasks", Integer
        .toString( context.getNumReduceTasks() ) );

    // String taskId = job.get( "mapred.task.id" );
    // String taskId = context.getTaskAttemptID().getTaskID().getId();
    String taskId = conf.get( "mapred.task.id" );
    m_helper.variableSpace.setVariable( "Internal.Hadoop.TaskId", taskId );
    // TODO: Verify if the string range holds true for all Hadoop distributions
    // Extract the node number from the task ID.
    // The consensus currently is that it's the part after the last underscore.
    //
    // Examples:
    // job_201208090841_9999
    // job_201208090841_10000
    //

    String nodeNumber;
    if ( Const.isEmpty( taskId ) ) {
      nodeNumber = "0";
    } else {
      int lastUnderscoreIndex = taskId.lastIndexOf( "_" );
      if ( lastUnderscoreIndex >= 0 ) {
        nodeNumber = taskId.substring( lastUnderscoreIndex + 1 );
      } else {
        nodeNumber = "0";
      }
    }

    // get rid of zeroes.
    //
    m_helper.variableSpace
        .setVariable( "Internal.Hadoop.NodeNumber", Integer.toString( Integer.valueOf( nodeNumber ) ) );
  }

  @Override
  public void run( Context context ) throws IOException {
    setup( context );
    try {
      if ( m_helper.trans == null ) {
        throw new RuntimeException( "Error initializing transformation.  See error log." ); //$NON-NLS-1$
      } else {
        // Clean up old logging
        KettleLogStore.discardLines( m_helper.trans.getLogChannelId(), true );
      }

      // Create a copy of trans so we don't continue to add new TransListeners and run into a
      // ConcurrentModificationException
      // when this mapper is reused "quickly"
      m_helper.trans = MRUtil.recreateTrans( m_helper.trans );

      String logLinePrefix = getClass().getName() + ".run: ";
      PentahoMapreduceHelper.setDebugStatus( context, logLinePrefix + " The transformation was just recreated.",
          m_helper.debug );

      // share the variables from the PDI job.
      // we do this here instead of in createTrans() as MRUtil.recreateTrans() wil not
      // copy "execution" trans information.
      if ( m_helper.variableSpace != null ) {
        PentahoMapreduceHelper.setDebugStatus( context, "Sharing the VariableSpace from the PDI job.", m_helper.debug );
        m_helper.trans.shareVariablesWith( m_helper.variableSpace );

        if ( m_helper.debug ) {

          // list the variables
          List<String> variables = Arrays.asList( m_helper.trans.listVariables() );
          Collections.sort( variables );

          if ( variables != null ) {
            PentahoMapreduceHelper.setDebugStatus( context, "Variables: ", m_helper.debug );
            for ( String variable : variables ) {
              PentahoMapreduceHelper.setDebugStatus( context, "     " + variable + " = "
                  + m_helper.trans.getVariable( variable ), m_helper.debug );
            }
          }
        }
      } else {
        PentahoMapreduceHelper.setDebugStatus( context,
            "variableSpace is null.  We are not going to share it with the trans.", m_helper.debug );
      }

      // set the trans' log level if we have our's set
      if ( m_helper.logLevel != null ) {
        PentahoMapreduceHelper.setDebugStatus( context,
            "Setting the trans.logLevel to " + m_helper.logLevel.toString(), m_helper.debug );
        m_helper.trans.setLogLevel( m_helper.logLevel );
      } else {
        PentahoMapreduceHelper.setDebugStatus( context, "logLevel is null.  The trans log level will not be set.",
            m_helper.debug );
      }

      // get the first key and value
      context.nextKeyValue();
      K1 key = context.getCurrentKey();
      V1 value = context.getCurrentValue();

      PentahoMapreduceHelper.setDebugStatus( context, "Preparing transformation for execution", m_helper.debug );
      m_helper.trans.prepareExecution( null );

      try {
        PentahoMapreduceHelper.setDebugStatus( context, "Locating output step: " + m_helper.mapOutputStepName,
            m_helper.debug );
        StepInterface outputStep = m_helper.trans.findRunThread( m_helper.mapOutputStepName );

        if ( outputStep != null ) {
          m_helper.rowCollector =
              new PentahoMapreduceOutputCollectorRowListener<K2, V2>( context, m_helper.outClassK, m_helper.outClassV,
                  m_helper.debug );

          // rowCollector = OutputCollectorRowListener.build(output, outputRowMeta, outClassK, outClassV, reporter,
          // debug);
          outputStep.addRowListener( m_helper.rowCollector );

          RowMeta injectorRowMeta = new RowMeta();
          RowProducer rowProducer = null;
          TypeConverterFactory typeConverterFactory = new TypeConverterFactory();
          ITypeConverter inConverterK = null;
          ITypeConverter inConverterV = null;

          PentahoMapreduceHelper.setDebugStatus( context, "Locating input step: " + m_helper.mapInputStepName,
              m_helper.debug );

          if ( m_helper.mapInputStepName != null ) {
            // Setup row injection
            rowProducer = m_helper.trans.addRowProducer( m_helper.mapInputStepName, 0 );
            StepInterface inputStep = rowProducer.getStepInterface();
            StepMetaInterface inputStepMeta = inputStep.getStepMeta().getStepMetaInterface();

            InKeyValueOrdinals inOrdinals = null;
            if ( inputStepMeta instanceof BaseStepMeta ) {
              PentahoMapreduceHelper.setDebugStatus( context,
                  "Generating converters from RowMeta for injection into the mapper transformation", m_helper.debug );

              // Use getFields(...) to get the row meta and therefore the expected input types
              inputStepMeta.getFields( injectorRowMeta, null, null, null, null );

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

              // Get a converter for the Value if the value meta has a concrete Java class we can use.
              // If no converter can be found here we wont do any type conversion.
              if ( injectorRowMeta.getValueMeta( inOrdinals.getValueOrdinal() ) != null ) {
                inConverterV =
                    typeConverterFactory.getConverter( value.getClass(), injectorRowMeta.getValueMeta( inOrdinals
                        .getValueOrdinal() ) );
              }
            }

            m_helper.trans.startThreads();
            if ( rowProducer != null ) {
              do {
                key = context.getCurrentKey();
                value = context.getCurrentValue();
                if ( inOrdinals != null ) {
                  m_helper.injectValue( key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals
                      .getValueOrdinal(), inConverterV, injectorRowMeta, rowProducer, context );
                } else {
                  m_helper.injectValue( key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, context );
                }
              } while ( context.nextKeyValue() );

              rowProducer.finished();
            }

            m_helper.trans.waitUntilFinished();
            PentahoMapreduceHelper.setDebugStatus( context, "Mapper transformation has finished", m_helper.debug );
            if ( m_helper.trans.getErrors() > 0 ) {
              PentahoMapreduceHelper.setDebugStatus( context, "Errors detected for mapper transformation",
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
              throw new Exception( "Errors were detected for mapper transformation:\n\n" + buff.toString() );
            }

          } else {
            PentahoMapreduceHelper.setDebugStatus( context, "No input stepname was defined", m_helper.debug );
          }

          if ( m_helper.getException() != null ) {
            PentahoMapreduceHelper.setDebugStatus( context, "An exception was generated by the mapper transformation",
                m_helper.debug );
            // Bubble the exception from within Kettle to Hadoop
            throw m_helper.getException();
          }

        } else {
          if ( m_helper.mapOutputStepName != null ) {
            PentahoMapreduceHelper.setDebugStatus( context, "Output step [" + m_helper.mapOutputStepName
                + "]could not be found", m_helper.debug );
            throw new KettleException( "Output step not defined in transformation" );
          } else {
            PentahoMapreduceHelper.setDebugStatus( context, "Output step name not specified", m_helper.debug );
          }
        }
      } finally {
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
    } catch ( Exception e ) {
      e.printStackTrace( System.err );
      PentahoMapreduceHelper.setDebugStatus( context, "An exception was generated by the mapper task", m_helper.debug );
      throw new IOException( e );
    }
    context.setStatus( "Completed processing record" );
  }
}
