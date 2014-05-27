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

import static org.pentaho.hadoop.shim.api.Configuration.STRING_COMBINE_SINGLE_THREADED;
import static org.pentaho.hadoop.shim.api.Configuration.STRING_REDUCE_SINGLE_THREADED;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

import com.thoughtworks.xstream.XStream;

public class PentahoMapreduceHelper<K, V> {

  public static enum Counter {
    INPUT_RECORDS, OUTPUT_RECORDS, OUT_RECORD_WITH_NULL_KEY, OUT_RECORD_WITH_NULL_VALUE
  }

  public static class PentahoMapreduceOutputCollectorRowListener<K, V> extends RowAdapter {
    private final boolean debug;

    private final TaskInputOutputContext<?, ?, K, V> context;

    private final Class<K> outClassK;

    private final Class<V> outClassV;

    // private OutputCollector<K, V> output;

    private OutKeyValueOrdinals outOrdinals;

    private final TypeConverterFactory typeConverterFactory;

    private Exception exception;

    public PentahoMapreduceOutputCollectorRowListener( TaskInputOutputContext<?, ?, K, V> context, Class<K> outClassK,
        Class<V> outClassV, boolean debug ) {
      this.context = context;
      this.outClassK = outClassK;
      this.outClassV = outClassV;
      this.debug = debug;

      this.typeConverterFactory = new TypeConverterFactory();

      outOrdinals = null;
    }

    @Override
    public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
      try {
        /*
         * Operation: Column 1: Key (convert to outClassK) Column 2: Value (convert to outClassV)
         */
        if ( row != null && !rowMeta.isEmpty() && rowMeta.size() >= 2 ) {
          if ( outOrdinals == null ) {
            outOrdinals = new OutKeyValueOrdinals( rowMeta );

            if ( outOrdinals.getKeyOrdinal() < 0 || outOrdinals.getValueOrdinal() < 0 ) {
              throw new KettleException( "outKey or outValue is not defined in transformation output stream" ); //$NON-NLS-1$
            }
          }

          // TODO Implement type safe converters

          if ( debug ) {
            PentahoMapreduceHelper
                .setDebugStatus(
                    context,
                    "Begin conversion of output key [from:" + ( row[outOrdinals.getKeyOrdinal()] == null ? null : row[outOrdinals.getKeyOrdinal()].getClass() ) + "] [to:" + outClassK + "]", debug ); //$NON-NLS-1$ //$NON-NLS-2$
          }
          PentahoMapreduceHelper.setDebugStatus( context, "getConverter: "
              + ( row[outOrdinals.getKeyOrdinal()] == null ? null : row[outOrdinals.getKeyOrdinal()].getClass() ),
              debug );
          PentahoMapreduceHelper.setDebugStatus( context, "out class: " + outClassK, debug );

          ITypeConverter converter =
              typeConverterFactory.getConverter( row[outOrdinals.getKeyOrdinal()] == null ? null : row[outOrdinals
                  .getKeyOrdinal()].getClass(), outClassK );
          PentahoMapreduceHelper.setDebugStatus( context, "ordinals key: " + outOrdinals.getKeyOrdinal(), debug );
          PentahoMapreduceHelper.setDebugStatus( context, "rowMeta: " + rowMeta, debug );
          PentahoMapreduceHelper.setDebugStatus( context, "rowMeta: " + rowMeta.getMetaXML(), debug );
          PentahoMapreduceHelper.setDebugStatus( context,
              "meta: " + rowMeta.getValueMeta( outOrdinals.getKeyOrdinal() ), debug );
          PentahoMapreduceHelper.setDebugStatus( context, "key: " + row[outOrdinals.getKeyOrdinal()], debug );
          Object outKey =
              converter.convert( rowMeta.getValueMeta( outOrdinals.getKeyOrdinal() ), row[outOrdinals.getKeyOrdinal()] );

          if ( debug ) {
            PentahoMapreduceHelper.setDebugStatus( context,
                "Begin conversion of output value [from:" + ( row[outOrdinals.getValueOrdinal()] == null ? null //$NON-NLS-1$
                    : row[outOrdinals.getValueOrdinal()].getClass() ) + "] [to:" + outClassV + "]", debug ); //$NON-NLS-1$ //$NON-NLS-2$
          }
          ITypeConverter valueConverter =
              typeConverterFactory.getConverter( row[outOrdinals.getValueOrdinal()] == null ? null : row[outOrdinals
                  .getValueOrdinal()].getClass(), outClassV );
          PentahoMapreduceHelper.setDebugStatus( context, "ordinals value: " + outOrdinals.getValueOrdinal(), debug );
          PentahoMapreduceHelper.setDebugStatus( context, "rowMeta: " + rowMeta, debug );
          PentahoMapreduceHelper.setDebugStatus( context, "rowMeta: " + rowMeta.getMetaXML(), debug );
          PentahoMapreduceHelper.setDebugStatus( context, "meta: "
              + rowMeta.getValueMeta( outOrdinals.getValueOrdinal() ), debug );
          PentahoMapreduceHelper.setDebugStatus( context, "value: " + row[outOrdinals.getValueOrdinal()], debug );
          Object outVal =
              valueConverter.convert( rowMeta.getValueMeta( outOrdinals.getValueOrdinal() ), row[outOrdinals
                  .getValueOrdinal()] );

          if ( outKey != null && outVal != null ) {
            if ( debug ) {
              PentahoMapreduceHelper.setDebugStatus( context,
                  "Collecting output record [" + outKey + "] - [" + outVal + "]", debug ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
            // TODO Implement type safe converters
            @SuppressWarnings( "unchecked" )
            K k = (K) outKey;
            // TODO Implement type safe converters
            @SuppressWarnings( "unchecked" )
            V v = (V) outVal;
            context.write( k, v );
          } else {
            if ( outKey == null ) {
              if ( debug ) {
                PentahoMapreduceHelper.setDebugStatus( context, "Transformation returned a null key", debug ); //$NON-NLS-1$
              }
              // reporter.incrCounter( Counter.OUT_RECORD_WITH_NULL_KEY, 1 );
              context.getCounter( Counter.OUT_RECORD_WITH_NULL_KEY ).increment( 1 );
            }
            if ( outVal == null ) {
              if ( debug ) {
                PentahoMapreduceHelper.setDebugStatus( context, "Transformation returned a null value", debug ); //$NON-NLS-1$
              }
              context.getCounter( Counter.OUT_RECORD_WITH_NULL_VALUE ).increment( 1 );
              // reporter.incrCounter( Counter.OUT_RECORD_WITH_NULL_VALUE, 1 );
            }
          }
        } else {
          if ( row == null || rowMeta.isEmpty() ) {
            if ( debug ) {
              PentahoMapreduceHelper.setDebugStatus( context, "Invalid row received from transformation", debug ); //$NON-NLS-1$
            }
          } else if ( rowMeta.size() < 2 ) {
            if ( debug ) {
              PentahoMapreduceHelper.setDebugStatus( context,
                  "Invalid row format. Expected key/value columns, but received " + rowMeta.size() //$NON-NLS-1$
                      + " columns", debug ); //$NON-NLS-1$
            }
          } else {
            OutKeyValueOrdinals outOrdinals = new OutKeyValueOrdinals( rowMeta );
            if ( outOrdinals.getKeyOrdinal() < 0 || outOrdinals.getValueOrdinal() < 0 ) {
              if ( debug ) {
                PentahoMapreduceHelper.setDebugStatus( context,
                    "outKey or outValue is missing from the transformation output step", debug ); //$NON-NLS-1$
              }
            }
            if ( debug ) {
              PentahoMapreduceHelper.setDebugStatus( context,
                  "Unknown issue with received data from transformation", debug ); //$NON-NLS-1$
            }
          }
        }
      } catch ( Exception ex ) {
        PentahoMapreduceHelper.setDebugStatus( context, "Unexpected exception recieved: " + ex.getMessage(), debug ); //$NON-NLS-1$
        exception = ex;
        throw new RuntimeException( ex );
      }
    }

    /**
     * @return The exception thrown from {@link #rowWrittenEvent(RowMetaInterface, Object[])}.
     */
    public Exception getException() {
      return exception;
    }
  }

  protected String transMapXml;
  protected String transCombinerXml;
  protected String transReduceXml;

  protected String mapInputStepName;
  protected String combinerInputStepName;
  protected String reduceInputStepName;

  protected String mapOutputStepName;
  protected String combinerOutputStepName;
  protected String reduceOutputStepName;

  protected VariableSpace variableSpace = null;

  protected Class<K> outClassK;
  protected Class<V> outClassV;

  protected String id = UUID.randomUUID().toString();

  protected boolean debug = false;

  protected LogLevel logLevel;

  // the transformation that will be used as a mapper or reducer
  protected Trans trans;

  // One of these is what trans is to be used as
  public static enum MROperations {
    Map, Combine, Reduce
  };

  // we set this to what this object is being used for - map or reduce
  protected MROperations mrOperation;

  // TODO - need a new one of these
  protected PentahoMapreduceOutputCollectorRowListener<K, V> rowCollector;

  protected boolean combineSingleThreaded;
  protected boolean reduceSingleThreaded;

  public void setup( TaskInputOutputContext context ) {
    debug = context.getConfiguration().get( "debug" ).equals( "true" );

    transMapXml = context.getConfiguration().get( "transformation-map-xml" );
    transCombinerXml = context.getConfiguration().get( "transformation-combiner-xml" );
    transReduceXml = context.getConfiguration().get( "transformation-reduce-xml" );
    mapInputStepName = context.getConfiguration().get( "transformation-map-input-stepname" );
    mapOutputStepName = context.getConfiguration().get( "transformation-map-output-stepname" );
    combinerInputStepName = context.getConfiguration().get( "transformation-combiner-input-stepname" );
    combinerOutputStepName = context.getConfiguration().get( "transformation-combiner-output-stepname" );
    combineSingleThreaded = isCombinerSingleThreaded( context );
    reduceInputStepName = context.getConfiguration().get( "transformation-reduce-input-stepname" );
    reduceOutputStepName = context.getConfiguration().get( "transformation-reduce-output-stepname" );
    reduceSingleThreaded = isReducerSingleThreaded( context );
    String xmlVariableSpace = context.getConfiguration().get( "variableSpace" );

    if ( !Const.isEmpty( xmlVariableSpace ) ) {
      setDebugStatus( "PentahoMapReduceBase. variableSpace was retrieved from the job.  The contents: ", debug );

      // deserialize from xml to variable space
      XStream xStream = new XStream();

      if ( xStream != null ) {
        setDebugStatus( "PentahoMapReduceBase: Setting classes variableSpace property.: ", debug );
        variableSpace = (VariableSpace) xStream.fromXML( xmlVariableSpace );
      }
    } else {
      setDebugStatus( "PentahoMapReduceBase: The PDI Job's variable space was not found in the job configuration.",
          debug );
      variableSpace = new Variables();
    }

    switch ( mrOperation ) {
      case Map:
      case Combine:
        outClassK = (Class<K>) context.getMapOutputKeyClass();
        outClassV = (Class<V>) context.getMapOutputValueClass();
        break;
      case Reduce:
        outClassK = (Class<K>) context.getOutputKeyClass();
        outClassV = (Class<V>) context.getOutputValueClass();
        break;
      default:
        throw new IllegalArgumentException( "Unsupported MapReduce operation: " + mrOperation );
    }

    if ( debug ) {
      System.out.println( "Job configuration>" );
      System.out.println( "Output key class: " + outClassK.getName() );
      System.out.println( "Output value class: " + outClassV.getName() );
    }

    // set the log level to what the level of the job is
    String stringLogLevel = context.getConfiguration().get( "logLevel" );
    if ( !Const.isEmpty( stringLogLevel ) ) {
      logLevel = LogLevel.valueOf( stringLogLevel );
      setDebugStatus( "Log level set to " + stringLogLevel, debug );
    } else {
      System.out.println( "Could not retrieve the log level from the job configuration.  logLevel will not be set." );
    }

    createTrans( context );
  }

  @Deprecated
  /**
   * Use the other injectValue method - The parameters have been arranged to be more uniform
   */
  public void injectValue( Object key, ITypeConverter inConverterK, ITypeConverter inConverterV,
      RowMeta injectorRowMeta, RowProducer rowProducer, Object value, TaskInputOutputContext context ) throws Exception {
    injectValue( key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, context );
  }

  public void injectValue( Object key, ITypeConverter inConverterK, Object value, ITypeConverter inConverterV,
      RowMetaInterface injectorRowMeta, RowProducer rowProducer, TaskInputOutputContext context ) throws Exception {

    injectValue( key, 0, inConverterK, value, 1, inConverterV, injectorRowMeta, rowProducer, context );
  }

  public void injectValue( Object key, int keyOrdinal, ITypeConverter inConverterK, Object value, int valueOrdinal,
      ITypeConverter inConverterV, RowMetaInterface injectorRowMeta, RowProducer rowProducer,
      TaskInputOutputContext context ) throws Exception {
    Object[] row = new Object[injectorRowMeta.size()];
    row[keyOrdinal] =
        inConverterK != null ? inConverterK.convert( injectorRowMeta.getValueMeta( keyOrdinal ), key ) : key;
    row[valueOrdinal] =
        inConverterV != null ? inConverterV.convert( injectorRowMeta.getValueMeta( valueOrdinal ), value ) : value;

    if ( debug ) {
      setDebugStatus( context, "Injecting input record [" + row[keyOrdinal] + "] - [" + row[valueOrdinal] + "]", debug );
    }

    rowProducer.putRow( injectorRowMeta, row );
  }

  protected void createTrans( final TaskInputOutputContext conf ) {

    if ( mrOperation == null ) {
      throw new RuntimeException(
          "Map or reduce operation has not been specified.  Call setMRType from implementing classes constructor." );
    }

    try {
      if ( mrOperation.equals( MROperations.Map ) ) {
        setDebugStatus( "Creating a transformation for a map.", debug );
        trans = MRUtil.getTrans( conf.getConfiguration(), transMapXml, false );
      } else if ( mrOperation.equals( MROperations.Combine ) ) {
        setDebugStatus( "Creating a transformation for a combiner.", debug );
        trans = MRUtil.getTrans( conf.getConfiguration(), transCombinerXml, isCombinerSingleThreaded( conf ) );
      } else if ( mrOperation.equals( MROperations.Reduce ) ) {
        setDebugStatus( "Creating a transformation for a reduce.", debug );
        trans = MRUtil.getTrans( conf.getConfiguration(), transReduceXml, isReducerSingleThreaded( conf ) );
      }
    } catch ( KettleException ke ) {
      throw new RuntimeException( "Error loading transformation for " + mrOperation, ke ); //$NON-NLS-1$
    }

  }

  private boolean isCombinerSingleThreaded( final TaskAttemptContext conf ) {
    return "true".equalsIgnoreCase( conf.getConfiguration().get( STRING_COMBINE_SINGLE_THREADED ) );
  }

  private boolean isReducerSingleThreaded( final TaskAttemptContext conf ) {
    return "true".equalsIgnoreCase( conf.getConfiguration().get( STRING_REDUCE_SINGLE_THREADED ) );
  }

  public static void setDebugStatus( TaskInputOutputContext context, String message, boolean debug ) {
    if ( debug ) {
      System.out.println( message );
      context.setStatus( message );
    }
  }

  private static void setDebugStatus( String message, boolean debug ) {
    if ( debug ) {
      System.out.println( message );
    }
  }

  /**
   * share the variables from the PDI job. we do this here instead of in createTrans() as MRUtil.recreateTrans() will
   * not copy "execution" trans information.
   */
  public void shareVariableSpaceWithTrans( TaskInputOutputContext context ) {
    if ( variableSpace != null ) {
      PentahoMapreduceHelper.setDebugStatus( context, "Sharing the VariableSpace from the PDI job.", debug );
      trans.shareVariablesWith( variableSpace );

      if ( debug ) {

        // list the variables
        List<String> variables = Arrays.asList( trans.listVariables() );
        Collections.sort( variables );

        if ( variables != null ) {
          PentahoMapreduceHelper.setDebugStatus( context, "Variables: ", debug );
          for ( String variable : variables ) {
            PentahoMapreduceHelper.setDebugStatus( context, "     " + variable + " = " + trans.getVariable( variable ),
                debug );
          }
        }
      }
    } else {
      PentahoMapreduceHelper.setDebugStatus( context,
          "variableSpace is null.  We are not going to share it with the trans.", debug );
    }
  }

  /**
   * set the trans' log level if we have our's set
   * 
   * @param context
   *          context
   */
  public void setTransLogLevel( TaskInputOutputContext context ) {
    if ( logLevel != null ) {
      PentahoMapreduceHelper.setDebugStatus( context, "Setting the trans.logLevel to " + logLevel.toString(), debug );
      trans.setLogLevel( logLevel );
    } else {
      PentahoMapreduceHelper.setDebugStatus( context, getClass().getName()
          + ".logLevel is null.  The trans log level will not be set.", debug );
    }
  }

  public void setMRType( MROperations mrOperation ) {
    this.mrOperation = mrOperation;
  }

  public String getTransMapXml() {
    return transMapXml;
  }

  public void setTransMapXml( String transMapXml ) {
    this.transMapXml = transMapXml;
  }

  public String getTransCombinerXml() {
    return transCombinerXml;
  }

  public void setCombinerMapXml( String transCombinerXml ) {
    this.transCombinerXml = transCombinerXml;
  }

  public String getTransReduceXml() {
    return transReduceXml;
  }

  public void setTransReduceXml( String transReduceXml ) {
    this.transReduceXml = transReduceXml;
  }

  public String getMapInputStepName() {
    return mapInputStepName;
  }

  public void setMapInputStepName( String mapInputStepName ) {
    this.mapInputStepName = mapInputStepName;
  }

  public String getMapOutputStepName() {
    return mapOutputStepName;
  }

  public void setMapOutputStepName( String mapOutputStepName ) {
    this.mapOutputStepName = mapOutputStepName;
  }

  public String getCombinerInputStepName() {
    return combinerInputStepName;
  }

  public void setCombinerInputStepName( String combinerInputStepName ) {
    this.combinerInputStepName = combinerInputStepName;
  }

  public String getCombinerOutputStepName() {
    return combinerOutputStepName;
  }

  public void setCombinerOutputStepName( String combinerOutputStepName ) {
    this.combinerOutputStepName = combinerOutputStepName;
  }

  public String getReduceInputStepName() {
    return reduceInputStepName;
  }

  public void setReduceInputStepName( String reduceInputStepName ) {
    this.reduceInputStepName = reduceInputStepName;
  }

  public String getReduceOutputStepName() {
    return reduceOutputStepName;
  }

  public void setReduceOutputStepName( String reduceOutputStepName ) {
    this.reduceOutputStepName = reduceOutputStepName;
  }

  public Class<?> getOutClassK() {
    return outClassK;
  }

  public void setOutClassK( Class<K> outClassK ) {
    this.outClassK = outClassK;
  }

  public Class<?> getOutClassV() {
    return outClassV;
  }

  public void setOutClassV( Class<V> outClassV ) {
    this.outClassV = outClassV;
  }

  public Trans getTrans() {
    return trans;
  }

  public void setTrans( Trans trans ) {
    this.trans = trans;
  }

  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public Exception getException() {
    return rowCollector != null ? rowCollector.getException() : null;
  }
}
