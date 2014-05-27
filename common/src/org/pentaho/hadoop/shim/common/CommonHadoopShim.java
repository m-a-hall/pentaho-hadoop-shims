/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Driver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.jdbc.HiveDriver;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.VersionInfo;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceGenericTransCombiner;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceGenericTransMapper;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceGenericTransReducer;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.DistributedCacheUtil;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.mapred.CustomJobConfigurer;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;
import org.pentaho.hadoop.shim.common.mapred.MapreduceJobExecutionProxy;
import org.pentaho.hadoop.shim.common.mapred.RunningJobProxy;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hdfs.vfs.HDFSFileProvider;

public class CommonHadoopShim implements HadoopShim {

  private DistributedCacheUtil dcUtil;

  @SuppressWarnings( "serial" )
  protected static Map<String, Class<? extends Driver>> JDBC_DRIVER_MAP =
      new HashMap<String, Class<? extends Driver>>() {
        {
          put( "hive", org.apache.hadoop.hive.jdbc.HiveDriver.class );
        }
      };

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 1, 0 );
  }

  @Override
  public String getHadoopVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    fsm.addProvider( config, "hdfs", config.getIdentifier(), new HDFSFileProvider() );
    setDistributedCacheUtil( new DistributedCacheUtilImpl( config ) );
  }

  @Override
  public Driver getHiveJdbcDriver() {
    try {
      return new HiveDriver();
    } catch ( Exception ex ) {
      throw new RuntimeException( "Unable to load Hive JDBC driver", ex );
    }
  }

  @Override
  public Driver getJdbcDriver( String driverType ) {
    try {
      Class<? extends Driver> clazz = JDBC_DRIVER_MAP.get( driverType );
      if ( clazz != null ) {
        Driver newInstance = clazz.newInstance();
        Driver driverProxy = DriverProxyInvocationChain.getProxy( Driver.class, newInstance );
        return driverProxy;
      } else {
        throw new Exception( "JDBC driver of type '" + driverType + "' not supported" );
      }

    } catch ( Exception ex ) {
      throw new RuntimeException( "Unable to load JDBC driver of type: " + driverType, ex );
    }
  }

  @Override
  public Configuration createConfiguration() {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      // return new org.pentaho.hadoop.shim.common.ConfigurationProxy();
      return new org.pentaho.hadoop.shim.common.MapreduceJobProxy();
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public FileSystem getFileSystem( Configuration conf ) throws IOException {
    // Set the context class loader when instantiating the configuration
    // since org.apache.hadoop.conf.Configuration uses it to load resources
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return new FileSystemProxy( org.apache.hadoop.fs.FileSystem.get( ShimUtils.asConfiguration( conf ) ) );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  public void setDistributedCacheUtil( DistributedCacheUtil dcUtil ) {
    if ( dcUtil == null ) {
      throw new NullPointerException();
    }
    this.dcUtil = dcUtil;
  }

  @Override
  public DistributedCacheUtil getDistributedCacheUtil() throws ConfigurationException {
    if ( dcUtil == null ) {
      throw new ConfigurationException( BaseMessages.getString( CommonHadoopShim.class,
          "CommonHadoopShim.DistributedCacheUtilMissing" ) );
    }
    return dcUtil;
  }

  @Override
  public String[] getNamenodeConnectionInfo( Configuration c ) {
    URI namenode = org.apache.hadoop.fs.FileSystem.getDefaultUri( ShimUtils.asConfiguration( c ) );
    String[] result = new String[2];
    if ( namenode != null ) {
      result[0] = namenode.getHost();
      if ( namenode.getPort() != -1 ) {
        result[1] = String.valueOf( namenode.getPort() );
      }
    }
    return result;
  }

  @Override
  public String[] getJobtrackerConnectionInfo( Configuration c ) {
    String[] result = new String[2];
    if ( !"local".equals( c.get( "mapred.job.tracker", "local" ) ) ) {
      InetSocketAddress jobtracker = getJobTrackerAddress( c );
      result[0] = jobtracker.getHostName();
      result[1] = String.valueOf( jobtracker.getPort() );
    }
    return result;
  }

  public static InetSocketAddress getJobTrackerAddress( Configuration conf ) {
    String jobTrackerStr = conf.get( "mapred.job.tracker", "localhost:8012" );
    return NetUtils.createSocketAddr( jobTrackerStr );
  }

  @Override
  public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
      String jobtrackerPort, Configuration conf, List<String> logMessages ) throws Exception {

    if ( namenodeHost == null || namenodeHost.trim().length() == 0 ) {
      throw new Exception( "No hdfs host specified!" );
    }
    if ( jobtrackerHost == null || jobtrackerHost.trim().length() == 0 ) {
      throw new Exception( "No job tracker host specified!" );
    }

    if ( namenodePort == null || namenodePort.trim().length() == 0 ) {
      namenodePort = getDefaultNamenodePort();
      logMessages.add( "No hdfs port specified - using default: " + namenodePort );
    }

    if ( jobtrackerPort == null || jobtrackerPort.trim().length() == 0 ) {
      jobtrackerPort = getDefaultJobtrackerPort();
      logMessages.add( "No job tracker port specified - using default: " + jobtrackerPort );
    }

    String fsDefaultName = "hdfs://" + namenodeHost + ":" + namenodePort;
    String jobTracker = jobtrackerHost + ":" + jobtrackerPort;

    conf.set( "fs.default.name", fsDefaultName );
    conf.set( "mapred.job.tracker", jobTracker );
  }

  /**
   * @return the default port of the namenode
   */
  protected String getDefaultNamenodePort() {
    return "9000";
  }

  /**
   * @return the default port of the jobtracker
   */
  protected String getDefaultJobtrackerPort() {
    return "9001";
  }

  @Override
  public RunningJob submitJob( Configuration c ) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      Object underlying = c.getUnderlyingJobObject();

      // any custom configurers?
      String customConfigClass = c.get( "pentaho.custom.job.configurer" );
      if ( customConfigClass != null && customConfigClass.length() > 0 ) {
        try {
          Class<?> confC = Class.forName( customConfigClass );
          CustomJobConfigurer configurer = (CustomJobConfigurer) confC.newInstance();
          configurer.configure( underlying );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }

      if ( underlying instanceof org.apache.hadoop.mapreduce.Job ) {
        try {
          ( (org.apache.hadoop.mapreduce.Job) underlying ).submit();
          MapreduceJobExecutionProxy proxy =
              new MapreduceJobExecutionProxy( (org.apache.hadoop.mapreduce.Job) underlying );

          return proxy;
        } catch ( ClassNotFoundException e ) {
          throw new IOException( e );
        } catch ( InterruptedException e ) {
          throw new IOException( e );
        }
      } else {
        JobConf conf = ShimUtils.asConfiguration( c );
        JobClient jobClient = new JobClient( conf );
        return new RunningJobProxy( jobClient.submitJob( conf ) );
      }
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public Class<? extends Writable> getHadoopWritableCompatibleClass( ValueMetaInterface kettleType ) {
    return TypeConverterFactory.getWritableForKettleType( kettleType );
  }

  @Override
  public Class<? extends Writable> getNamedHadoopWritableClass( String className ) throws ClassNotFoundException {
    return TypeConverterFactory.getNamedWritable( className );
  }

  @Override
  public Class<?> getPentahoMapReduceCombinerClass() {
    return PentahoMapreduceGenericTransCombiner.class;
    // return GenericTransCombine.class;
  }

  @Override
  public Class<?> getPentahoMapReduceReducerClass() {
    return PentahoMapreduceGenericTransReducer.class;
    // return GenericTransReduce.class;
  }

  @Override
  public Class<?> getPentahoMapReduceMapRunnerClass() {
    return PentahoMapreduceGenericTransMapper.class;
    // return PentahoMapRunnable.class;
  }
}
