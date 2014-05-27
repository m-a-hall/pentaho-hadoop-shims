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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;
import org.pentaho.hbase.shim.api.KeyValueHolder;

/**
 * Converter for converting KeyValueHolder to the writable type KeyValue
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * 
 */
public class KeyValueHolderToKeyValueConverter implements ITypeConverter<KeyValueHolder, KeyValue> {

  @Override
  public boolean canConvert( Class from, Class to ) {
    return ( KeyValueHolder.class.equals( from ) && KeyValue.class.equals( to ) );
  }

  @Override
  public KeyValue convert( ValueMetaInterface meta, KeyValueHolder obj ) throws TypeConversionException {

    Long timestamp = obj.getTimestamp();
    KeyValue.Type t = KeyValue.Type.Put;
    switch ( obj.getType() ) {
      case Minimum:
        t = KeyValue.Type.Minimum;
        break;
      case Put:
        t = KeyValue.Type.Put;
        break;
      case Delete:
        t = KeyValue.Type.Delete;
        break;
      case DeleteColumn:
        t = KeyValue.Type.DeleteColumn;
        break;
      case DeleteFamliy:
        t = KeyValue.Type.DeleteFamily;
        break;
      case Maximum:
        t = KeyValue.Type.Maximum;
        break;
      default:
        t = KeyValue.Type.Put;
    }
    KeyValue kv =
        new KeyValue( obj.getKey(), obj.getColumnFamily(), obj.getQualifier(), ( timestamp == null
            ? HConstants.LATEST_TIMESTAMP : timestamp.longValue() ), t, obj.getValue() );
    return kv;
  }
}
