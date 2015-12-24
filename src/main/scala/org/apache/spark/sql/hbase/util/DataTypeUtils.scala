/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.sql.hbase.util

import java.sql.{Timestamp, Date}

import org.apache.hadoop.hbase.filter.{BinaryComparator, ByteArrayComparable}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, MutableRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Data Type conversion utilities
 */
object DataTypeUtils extends Logging {

  val supportedDataTypes = (IntegerType :: StringType :: LongType
    :: ShortType :: BooleanType :: ByteType
    :: DoubleType :: FloatType :: DateType
    :: TimestampType :: DecimalType :: StructType :: ArrayType :: MapType :: Nil).toSet

  val keySupportedDataTypes = (IntegerType :: StringType :: LongType
    :: ShortType :: BooleanType :: ByteType
    :: DoubleType :: FloatType :: DateType :: TimestampType :: DecimalType :: Nil).toSet

  /**
   * create binary comparator for the input expression
   * @param field the byte utility
   * @param expression the input expression
   * @return the constructed binary comparator
   */
  def getBinaryComparator(field: FieldData, expression: Literal): ByteArrayComparable = {
    field.getDataStorageFormat match {
      case FieldFactory.BINARY_FORMAT =>
        new BinaryComparator(field.getRawBytes(expression.value.asInstanceOf[field.InternalType]))
      case FieldFactory.HBASE_FORMAT =>
        new BinaryComparator(field.getRawBytes(expression.value.asInstanceOf[field.InternalType]))
      case FieldFactory.STRING_FORMAT =>
        val rawBytes = field.getRawBytes(expression.value.asInstanceOf[field.InternalType])
        expression.dataType match {
          case BooleanType => new BoolComparator(rawBytes)
          case ByteType => new ByteComparator(rawBytes)
          case DoubleType => new DoubleComparator(rawBytes)
          case FloatType => new FloatComparator(rawBytes)
          case IntegerType => new IntComparator(rawBytes)
          case LongType => new LongComparator(rawBytes)
          case ShortType => new ShortComparator(rawBytes)
          case DateType => new IntComparator(rawBytes)
          case TimestampType => new LongComparator(rawBytes)
          case _ => new BinaryComparator(rawBytes)
        }
    }
  }
}
