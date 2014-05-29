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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converter that converts an encoded byte array to an ImmutableBytesWritable.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class ByteArrayToImmutableBytesWritableConverter implements ITypeConverter<byte[], ImmutableBytesWritable> {

  @Override
  public boolean canConvert( Class from, Class to ) {
    return ( byte[].class.equals( from ) && ImmutableBytesWritable.class.equals( to ) );
  }

  @Override
  public ImmutableBytesWritable convert( ValueMetaInterface meta, byte[] obj ) throws TypeConversionException {

    ImmutableBytesWritable result = new ImmutableBytesWritable( obj );

    return result;
  }
}
