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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.hadoop.mapreduce.PentahoMapreduceHelper.MROperations;

public class PentahoMapreduceGenericTransCombiner<K extends WritableComparable<?>, V extends Iterable<Writable>, K2, V2>
    extends PentahoMapreduceGenericTransReducer<K, V, K2, V2> {

  public PentahoMapreduceGenericTransCombiner() throws KettleException {
    super();

    this.setMRType( MROperations.Combine );
  }

  @Override
  public boolean isSingleThreaded() {
    return m_helper.combineSingleThreaded;
  }

  @Override
  public String getInputStepName() {
    return m_helper.combinerInputStepName;
  }

  @Override
  public String getOutputStepName() {
    return m_helper.combinerOutputStepName;
  }
}
