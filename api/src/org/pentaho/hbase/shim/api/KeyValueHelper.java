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

package org.pentaho.hbase.shim.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper routines for storing and retrieving the payload of a HBase KeyValue in
 * a Map.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class KeyValueHelper implements Serializable {

  /**
   * For serialization
   */
  private static final long serialVersionUID = 7357162389078817494L;

  public static final int MINIMUM = 0;
  public static final int PUT = 1;
  public static final int DELETE = 2;
  public static final int DELETECOLUMN = 3;
  public static final int DELETEFAMILY = 4;
  public static final int MAXIMUM = 5;

  public static final String KEY = "key";
  public static final String COLUMN_FAMILY = "colFam";
  public static final String QUALIFIER = "qualifier";
  public static final String VALUE = "value";
  public static final String TIMESTAMP = "timestamp";
  public static final String TYPE = "type";

  public static Map<String, Object> valuesToMap(byte[] key, byte[] colFam,
    byte[] qualifier, byte[] value, Long timestamp, Integer type) {
    Map<String, Object> result = new HashMap<String, Object>();

    result.put(KEY, key);
    result.put(COLUMN_FAMILY, colFam);
    result.put(QUALIFIER, qualifier);
    result.put(VALUE, value);
    result.put(TIMESTAMP, timestamp);
    result.put(TYPE, type);

    return result;
  }

  public static byte[] getKey(Map<String, Object> valMap) {
    return (byte[]) valMap.get(KEY);
  }

  public static byte[] getColumnFamily(Map<String, Object> valMap) {
    return (byte[]) valMap.get(COLUMN_FAMILY);
  }

  public static byte[] getQualifier(Map<String, Object> valMap) {
    return (byte[]) valMap.get(QUALIFIER);
  }

  public static byte[] getValue(Map<String, Object> valMap) {
    return (byte[]) valMap.get(VALUE);
  }

  public static Long getTimestamp(Map<String, Object> valMap) {
    return (Long) valMap.get(TIMESTAMP);
  }

  public static Integer getType(Map<String, Object> valMap) {
    return (Integer) valMap.get(TYPE);
  }
}
