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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object HBaseKVHelper {

  /**
   * Takes a record, translate it into HBase row key column and value by matching with metadata
   * @param values record that as a sequence of string
   * @param relation HBaseRelation
   * @param keyBytes  output parameter, array of (key column and its type);
   * @param valueBytes array of (column family, column qualifier, value)
   */
  def string2KV(values: Seq[String],
                relation: HBaseRelation,
                lineBuffer: Seq[FieldData],
                keyBytes: Array[(Array[Byte], DataType)],
                valueBytes: Array[HBaseRawType]): Boolean = {

    relation.keyColumns.foreach(kc => {
      val ordinal = kc.ordinal
      val bytes = lineBuffer(ordinal).parseStringToTypeDataBytes(values(ordinal))
      if (bytes.isEmpty) {
        return false
      }
      keyBytes(kc.order) = (bytes, relation.output(ordinal).dataType)
    })
    for (i <- relation.nonKeyColumns.indices) {
      val nkc = relation.nonKeyColumns(i)
      val bytes =  {
        lineBuffer(nkc.ordinal).parseStringToTypeDataBytes(values(nkc.ordinal))
      }
      valueBytes(i) = bytes
    }
    true
  }


  /**
   * append one to the byte array
   * @param input the byte array
   * @return the modified byte array
   */
  def addOneString(input: HBaseRawType, separator: Byte): HBaseRawType = {
    val len = input.length
    val result = new HBaseRawType(len + 1)
    Array.copy(input, 0, result, 0, len)
    result(len) = (separator+1).asInstanceOf[Byte]
    result
  }

  /**
   * add one to the unsigned byte array
   * @param input the unsigned byte array
   * @return null if the byte array is all 0xff, otherwise increase by 1
   */
  def addOne(input: HBaseRawType): HBaseRawType = {
    val len = input.length
    val result = new HBaseRawType(len)
    Array.copy(input, 0, result, 0, len)
    var setValue = false
    for (index <- len - 1 to 0 by -1 if !setValue) {
      val item: Byte = input(index)
      if (item != 0xff.toByte) {
        setValue = true
        if ((item & 0x01.toByte) == 0.toByte) {
          result(index) = (item ^ 0x01.toByte).toByte
        } else if ((item & 0x02.toByte) == 0.toByte) {
          result(index) = (item ^ 0x03.toByte).toByte
        } else if ((item & 0x04.toByte) == 0.toByte) {
          result(index) = (item ^ 0x07.toByte).toByte
        } else if ((item & 0x08.toByte) == 0.toByte) {
          result(index) = (item ^ 0x0f.toByte).toByte
        } else if ((item & 0x10.toByte) == 0.toByte) {
          result(index) = (item ^ 0x1f.toByte).toByte
        } else if ((item & 0x20.toByte) == 0.toByte) {
          result(index) = (item ^ 0x3f.toByte).toByte
        } else if ((item & 0x40.toByte) == 0.toByte) {
          result(index) = (item ^ 0x7f.toByte).toByte
        } else {
          result(index) = (item ^ 0xff.toByte).toByte
        }
        // after increment, set remaining bytes to zero
        for (rest <- index + 1 until len) {
          result(rest) = 0x00.toByte
        }
      }
    }
    if (!setValue) null
    else result
  }


  /**
   * create a row key
   * @param row the generic row
   * @param dataTypeOfKeys sequence of data type
   * @return the row key
   */
  def makeRowKey(row: InternalRow, dataTypeOfKeys: Seq[DataType],
                 encodingFormat: String, extraParams: Map[String,String], keyFactory: KeyFactory): HBaseRawType = {
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) =>
        (FieldFactory.createFieldData(dataType, encodingFormat, FieldFactory.collectSeperators(extraParams)).
          getRowColumnInHBaseRawType(row, index), dataType)
    }

    keyFactory.encodingRawKeyColumns(rawKeyCol)
  }
}
