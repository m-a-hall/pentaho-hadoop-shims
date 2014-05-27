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

public class KeyValueHolder implements Serializable {

  /**
   * For serialization
   */
  private static final long serialVersionUID = 7357162389078817494L;

  public static enum Type {
    Minimum, Put, Delete, DeleteColumn, DeleteFamliy, Maximum;
  }

  /** Encoded key */
  protected byte[] m_key;

  /** Encoded column family */
  protected byte[] m_columnFamily;

  /** Encoded qualifier */
  protected byte[] m_qualifier;

  /** Encoded value */
  protected byte[] m_value;

  /** Timestamp - leave null to let HBase use its current timestamp */
  protected Long m_timestamp;

  /** Type of operation */
  protected Type m_type = Type.Put;

  public KeyValueHolder(byte[] key, byte[] colFam, byte[] qualifier,
    byte[] value) {
    this(key, colFam, qualifier, value, null, Type.Put);
  }

  public KeyValueHolder(byte[] key, byte[] colFam, byte[] qualifier,
    byte[] value, Long timestamp, Type type) {
    m_key = key;
    m_columnFamily = colFam;
    m_qualifier = qualifier;
    m_value = value;
    m_type = type;
  }

  public byte[] getKey() {
    return m_key;
  }

  public byte[] getColumnFamily() {
    return m_columnFamily;
  }

  public byte[] getQualifier() {
    return m_qualifier;
  }

  public byte[] getValue() {
    return m_value;
  }

  public Long getTimestamp() {
    return m_timestamp;
  }

  public Type getType() {
    return m_type;
  }
}
