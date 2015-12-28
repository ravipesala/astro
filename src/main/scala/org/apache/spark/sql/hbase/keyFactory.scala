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

package org.apache.spark.sql.hbase

import org.apache.spark.sql.types.{AtomicType, DataType, DecimalType, StringType}

/**
 * This interface is responsible for encoding and decoding of row key
 */
abstract class KeyFactory extends Serializable {

  val delimiter: Byte

  /**
   * create row key based on key columns information
   * @param rawKeyColumns sequence of byte array and data type representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[(HBaseRawType, DataType)]): HBaseRawType

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
                            startIndex: Int = 0): Seq[(Int, Int)]

}

class AstroKeyFactory extends KeyFactory {
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

}

class SimpleKeyFactory(val delimiter: Byte) extends KeyFactory {

  /**
   * create row key based on key columns information it will add delimiter as its delimiter
   * @param rawKeyColumns sequence of byte array and data type representing the key columns
   * @return array of bytes
   */
  def encodingRawKeyColumns(rawKeyColumns: Seq[(HBaseRawType, DataType)]): HBaseRawType = {
    var length = 0
    for (i <- 0 until rawKeyColumns.length) {
      length += rawKeyColumns(i)._1.length
      if (i < rawKeyColumns.length - 1) {
        length += 1
      }
    }
    val result = new HBaseRawType(length)
    var index = 0
    var kcIdx = 0
    for (rawKeyColumn <- rawKeyColumns) {
      Array.copy(rawKeyColumn._1, 0, result, index, rawKeyColumn._1.length)
      index += rawKeyColumn._1.length
      if (kcIdx < rawKeyColumns.length - 1) {
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
          pos = rowKey.indexOf(delimiter, index)
          if (pos == -1 || pos > limit) {
            // this is at the last dimension
            pos = limit
          }
          index = pos + 1
          (offset, pos - offset)
        }
    }
  }
}