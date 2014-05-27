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

package org.pentaho.hadoop.shim.common.mapred;

import java.io.IOException;
import java.lang.reflect.Method;

import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.api.mapred.TaskCompletionEvent;

public class MapreduceJobExecutionProxy implements RunningJob {

  protected org.apache.hadoop.mapreduce.Job m_delegate;

  public MapreduceJobExecutionProxy( org.apache.hadoop.mapreduce.Job delegate ) {
    if ( delegate == null ) {
      throw new NullPointerException();
    }
    m_delegate = delegate;
  }

  @Override
  public boolean isComplete() throws IOException {
    return m_delegate.isComplete();
  }

  @Override
  public void killJob() throws IOException {
    m_delegate.killJob();
  }

  @Override
  public boolean isSuccessful() throws IOException {
    return m_delegate.isSuccessful();
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents( int startIndex ) throws IOException {
    org.apache.hadoop.mapred.TaskCompletionEvent[] events = m_delegate.getTaskCompletionEvents( startIndex );

    TaskCompletionEvent[] wrapped = new TaskCompletionEvent[events.length];

    for ( int i = 0; i < wrapped.length; i++ ) {
      wrapped[i] = new TaskCompletionEventProxy( events[i] );
    }

    return wrapped;
  }

  @Override
  public String[] getTaskDiagnostics( Object taskAttemptId ) throws IOException {
    // There doesn't seem to be anything analogous to this in the mapreduce api
    return null;
  }

  @Override
  public float setupProgress() throws IOException {
    // setupProgress() does not exist in apache hadoop 20, but does in hadoop 1.x and higher
    // return m_delegate.setupProgress();

    try {
      Method m = m_delegate.getClass().getDeclaredMethod( "setupProgress", new Class[] {} );

      Object result = m.invoke( m_delegate, new Object[] {} );
      if ( result != null ) {
        return Float.parseFloat( result.toString() );
      }
    } catch ( Exception ex ) {
      // don't make a fuss
    }
    return 0;
  }

  @Override
  public float mapProgress() throws IOException {
    return m_delegate.mapProgress();
  }

  @Override
  public float reduceProgress() throws IOException {
    return m_delegate.reduceProgress();
  }
}
