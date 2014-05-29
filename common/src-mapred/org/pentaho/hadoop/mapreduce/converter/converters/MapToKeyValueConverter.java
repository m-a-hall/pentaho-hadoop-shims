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

package org.pentaho.hadoop.mapreduce.converter.converters;

import java.util.HashMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;
import org.pentaho.hbase.shim.api.KeyValueHelper;

/**
 * Converter for converting a HashMap (holding KeyValue byte array payload bits) to the writable type KeyValue
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class MapToKeyValueConverter implements ITypeConverter<HashMap<String, Object>, KeyValue> {

  @Override
  public boolean canConvert( Class from, Class to ) {
    return ( HashMap.class.equals( from ) && KeyValue.class.equals( to ) );
  }

  @Override
  public KeyValue convert( ValueMetaInterface meta, HashMap<String, Object> obj ) throws TypeConversionException {

    byte[] key = KeyValueHelper.getKey( obj );
    byte[] colFam = KeyValueHelper.getColumnFamily( obj );
    byte[] qualifier = KeyValueHelper.getQualifier( obj );
    byte[] value = KeyValueHelper.getValue( obj );
    Long timestamp = KeyValueHelper.getTimestamp( obj );
    Integer type = KeyValueHelper.getType( obj );

    KeyValue.Type t = KeyValue.Type.Put;
    switch ( type.intValue() ) {
      case KeyValueHelper.MINIMUM:
        t = KeyValue.Type.Minimum;
        break;
      case KeyValueHelper.PUT:
        t = KeyValue.Type.Put;
        break;
      case KeyValueHelper.DELETE:
        t = KeyValue.Type.Delete;
        break;
      case KeyValueHelper.DELETECOLUMN:
        t = KeyValue.Type.DeleteColumn;
        break;
      case KeyValueHelper.DELETEFAMILY:
        t = KeyValue.Type.DeleteFamily;
        break;
      case KeyValueHelper.MAXIMUM:
        t = KeyValue.Type.Maximum;
        break;
      default:
        t = KeyValue.Type.Put;
    }
    KeyValue kv =
        new KeyValue( key, colFam, qualifier,
            ( timestamp == null ? HConstants.LATEST_TIMESTAMP : timestamp.longValue() ), t, value );
    return kv;
  }
}
