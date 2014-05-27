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

package org.pentaho.hadoop.shim.common;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.Path;

public class MapreduceJobProxy extends org.apache.hadoop.mapred.JobConf implements Configuration {

  protected org.apache.hadoop.conf.Configuration m_conf = new org.apache.hadoop.conf.Configuration();
  protected String m_jobName;
  protected int m_numReduceTasks = 1;
  protected int m_numMapTasks = 1;
  protected Class m_jarByClass;
  protected Class m_mapOutputKeyClass;
  protected Class m_mapOutputValueClass;
  protected Class m_outputKeyClass;
  protected Class m_outputValueClass;
  protected Class m_mapperClass;
  protected Class m_combinerClass;
  protected Class m_reducerClass;
  protected Class m_mapRunnerClass;
  protected Class m_inputFormat;
  protected Class m_outputFormat;

  protected Path[] m_inputPaths;
  protected Path m_outputPath;

  protected String m_jar; // call getConfiguration() on Job to get a Configuration (which is actually an JobConf) - then
                          // call setJar on this

  public MapreduceJobProxy() {
    super();
    addResource( "hdfs-site.xml" );
    m_conf.addResource( "hdfs-site.xml" );
  }

  @Override
  public void setJobName( String name ) {
    m_jobName = name;
  }

  @Override
  public void set( String name, String value ) {
    m_conf.set( name, value );
  }

  @Override
  public String get( String name ) {
    return m_conf != null ? m_conf.get( name ) : null;
  }

  @Override
  public String get( String name, String defaultValue ) {
    return m_conf.get( name, defaultValue );
  }

  @Override
  public void setNumReduceTasks( int n ) {
    m_numReduceTasks = n;
  }

  @Override
  public void setNumMapTasks( int n ) {
    m_numMapTasks = n;
  }

  @Override
  public void setMapOutputKeyClass( Class c ) {
    m_mapOutputKeyClass = c;
  }

  @Override
  public void setMapOutputValueClass( Class c ) {
    m_mapOutputValueClass = c;
  }

  @Override
  public void setOutputKeyClass( Class c ) {
    m_outputKeyClass = c;
  }

  @Override
  public void setOutputValueClass( Class c ) {
    m_outputValueClass = c;
  }

  @Override
  public void setMapperClass( Class c ) {
    m_mapperClass = c;
  }

  @Override
  public void setCombinerClass( Class c ) {
    m_combinerClass = c;
  }

  @Override
  public void setReducerClass( Class c ) {
    m_reducerClass = c;
  }

  @Override
  public void setMapRunnerClass( Class c ) {
    m_mapRunnerClass = c;
  }

  @Override
  public void setInputFormat( Class inputFormat ) {
    m_inputFormat = inputFormat;
  }

  @Override
  public void setOutputFormat( Class outputFormat ) {
    m_outputFormat = outputFormat;
  }

  @Override
  public void setInputPaths( Path... paths ) {
    m_inputPaths = paths;
  }

  @Override
  public void setOutputPath( Path path ) {
    m_outputPath = path;
  }

  @Override
  public void setJar( String url ) {
    m_jar = url;
  }

  @Override
  public void setStrings( String name, String... values ) {
    m_conf.setStrings( name, values );
  }

  @Override
  public String getDefaultFileSystemURL() {
    return get( "fs.default.name", "" );
  }

  @Override
  public void setJarByClass( Class c ) {
    m_jarByClass = c;
  }

  @Override
  public Object getUnderlyingJobObject() {
    Job job = null;
    try {
      job = new Job( new org.apache.hadoop.conf.Configuration( m_conf ), m_jobName );

      job.setNumReduceTasks( m_numReduceTasks );
      ( (JobConf) job.getConfiguration() ).setNumMapTasks( m_numMapTasks );
      if ( m_mapOutputKeyClass != null ) {
        job.setMapOutputKeyClass( m_mapOutputKeyClass );
      }
      if ( m_mapOutputValueClass != null ) {
        job.setMapOutputValueClass( m_mapOutputValueClass );
      }
      if ( m_outputKeyClass != null ) {
        job.setOutputKeyClass( m_outputKeyClass );
      }
      if ( m_outputValueClass != null ) {
        job.setOutputValueClass( m_outputValueClass );
      }
      if ( m_mapperClass != null ) {
        job.setMapperClass( m_mapperClass );
      }
      if ( m_combinerClass != null ) {
        job.setCombinerClass( m_combinerClass );
      }
      if ( m_reducerClass != null ) {
        job.setReducerClass( m_reducerClass );
      }

      // MapRunner no longer applies under mapreduce as Mapper now has a run() method
      // We'll set the maprunner class as the mapper (overriding any mapper set via setMapperClass()
      if ( m_mapRunnerClass != null ) {
        job.setMapperClass( m_mapRunnerClass );
      }

      if ( m_inputFormat != null ) {
        job.setInputFormatClass( m_inputFormat );
      }
      if ( m_outputFormat != null ) {
        job.setOutputFormatClass( m_outputFormat );
      }
      FileOutputFormat.setOutputPath( job, ShimUtils.asPath( m_outputPath ) );

      if ( m_inputPaths != null ) {
        org.apache.hadoop.fs.Path[] actualPaths = new org.apache.hadoop.fs.Path[m_inputPaths.length];
        for ( int i = 0; i < actualPaths.length; i++ ) {
          actualPaths[i] = ShimUtils.asPath( m_inputPaths[i] );
        }

        FileInputFormat.setInputPaths( job, actualPaths );
      }

      if ( m_jar != null ) {
        ( (JobConf) job.getConfiguration() ).setJar( m_jar );
      }

      if ( m_jarByClass != null ) {
        job.setJarByClass( m_jarByClass );
      }
    } catch ( IOException e ) {
      throw new RuntimeException( e );
    }

    return job;
  }

}
