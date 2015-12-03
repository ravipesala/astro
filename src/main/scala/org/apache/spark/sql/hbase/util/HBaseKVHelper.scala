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
  val delimiter: Byte = 0

  /**
   * create row key based on key columns information
   * for strings, it will add '0x00' as its delimiter
   * @param rawKeyColumns sequence of byte array and data type representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[(HBaseRawType, DataType)]): HBaseRawType = {
    var length = 0
    for (i <- 0 until rawKeyColumns.length) {
      length += rawKeyColumns(i)._1.length
      if ((rawKeyColumns(i)._2 == StringType ||
        rawKeyColumns(i)._2 == DecimalType) &&
        i < rawKeyColumns.length - 1) {
        length += 1
      }
    }
    val result = new HBaseRawType(length)
    var index = 0
    var kcIdx = 0
    for (rawKeyColumn <- rawKeyColumns) {
      Array.copy(rawKeyColumn._1, 0, result, index, rawKeyColumn._1.length)
      index += rawKeyColumn._1.length
      if ((rawKeyColumn._2 == StringType ||
        rawKeyColumn._2 == DecimalType) &&
        kcIdx < rawKeyColumns.length - 1) {
        result(index) = delimiter
        index += 1
      }
      kcIdx += 1
    }
    result
  }

  /**
   * generate the sequence information of key columns from the byte array
   * @param rowKey array of bytes
   * @param keyColumns the sequence of key columns
   * @param keyLength rowkey length: specified if not negative
   * @return sequence of information in (offset, length) tuple
   */
  def decodingRawKeyColumns(rowKey: HBaseRawType,
                            keyColumns: Seq[KeyColumn],
                            keyLength: Int = -1,
                            startIndex: Int = 0): Seq[(Int, Int)] = {
    var index = startIndex
    var pos = 0
    val limit = if (keyLength < 0) {
      rowKey.length
    } else {
      index + keyLength
    }
    keyColumns.map {
      case c =>
        if (index >= limit) (-1, -1)
        else {
          val offset = index
          if (c.dataType == StringType || c.dataType == DecimalType) {
            pos = rowKey.indexOf(delimiter, index)
            if (pos == -1 || pos > limit) {
              // this is at the last dimension
              pos = limit
            }
            index = pos + 1
            (offset, pos - offset)
          } else {
            val length = c.dataType.asInstanceOf[AtomicType].defaultSize
            index += length
            (offset, length)
          }
        }
    }
  }

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
  def addOneString(input: HBaseRawType): HBaseRawType = {
    val len = input.length
    val result = new HBaseRawType(len + 1)
    Array.copy(input, 0, result, 0, len)
    result(len) = 0x01.asInstanceOf[Byte]
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
                 encodingFormat: String, extraParams: Map[String,String]): HBaseRawType = {
    val rawKeyCol = dataTypeOfKeys.zipWithIndex.map {
      case (dataType, index) =>
        (FieldFactory.createFieldData(dataType, encodingFormat, FieldFactory.collectSeperators(extraParams)).
          getRowColumnInHBaseRawType(row, index), dataType)
    }

    encodingRawKeyColumns(rawKeyCol)
  }
}
