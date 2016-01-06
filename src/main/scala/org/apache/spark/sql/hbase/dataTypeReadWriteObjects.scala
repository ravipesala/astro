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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.sql.{Date, Timestamp}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, GenericRowWithSchema, MutableRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object FieldFactory {

  val BINARY_FORMAT = "binaryformat"
  val STRING_FORMAT = "stringformat"
  val HBASE_FORMAT = "hbaseformat"
  val COLLECTION_SEPERATOR = "collectionSeperator"
  val MAPKEY_SEPERATOR = "mapkeySeperator"

  def collectSeperators(parameters: Map[String, String]) = {
    val separatorCandidates: ArrayBuffer[Byte] = new ArrayBuffer[Byte]()
    separatorCandidates += getByte(parameters.getOrElse(FieldFactory.COLLECTION_SEPERATOR, null), 2.toByte)
    separatorCandidates += getByte(parameters.getOrElse(FieldFactory.MAPKEY_SEPERATOR, null), 3.toByte)

    //use only control chars that are very unlikely to be part of the string
    // the following might/likely to be used in text files for strings
    // 9 (horizontal tab, HT, \t, ^I)
    // 10 (line feed, LF, \n, ^J),
    // 12 (form feed, FF, \f, ^L),
    // 13 (carriage return, CR, \r, ^M),
    // 27 (escape, ESC, \e [GCC only], ^[).
    var b: Byte = 4
    for (b <- 4 to 8) {
      separatorCandidates += b.toByte
    }
    separatorCandidates += 11.toByte
    for (b <- 14 to 26) {
      separatorCandidates += b.toByte
    }
    for (b <- 28 to 31) {
      separatorCandidates += b.toByte
    }
    for (b <- 128 to -1) {
      separatorCandidates += b.toByte
    }
    separatorCandidates.toArray[Byte]
  }

  private def getByte(altValue: String, defaultVal: Byte): Byte = {
    if (altValue != null && altValue.length > 0) {
      try {
        return altValue.toByte
      }
      catch {
        case e: NumberFormatException =>
          return altValue.charAt(0).toByte
      }
    }
    defaultVal
  }

  /**
   * It creates the fieldData object for corresponding datatype and encoding.
   * @param dataType
   * @param encodingFormat
   * @param separators
   * @param level
   * @param hiveStore
   * @return FieldData
   */
  def createFieldData(dataType: DataType,
                      encodingFormat: String,
                      separators: Array[Byte],
                      level: Int = 0, hiveStore: Boolean = false): FieldData = {
    encodingFormat match {
      case BINARY_FORMAT =>
        dataType match {
          case ByteType => AstroBinaryFormat.ByteDataField(Bytes.SIZEOF_BYTE, ByteType)
          case BooleanType => AstroBinaryFormat.BooleanDataField(Bytes.SIZEOF_BOOLEAN, BooleanType)
          case ShortType => AstroBinaryFormat.ShortDataField(Bytes.SIZEOF_SHORT, ShortType)
          case IntegerType => AstroBinaryFormat.IntDataField(Bytes.SIZEOF_INT, IntegerType)
          case LongType => AstroBinaryFormat.LongDataField(Bytes.SIZEOF_LONG, LongType)
          case FloatType => AstroBinaryFormat.FloatDataField(Bytes.SIZEOF_FLOAT, FloatType)
          case DoubleType => AstroBinaryFormat.DoubleDataField(Bytes.SIZEOF_DOUBLE, DoubleType)
          case DateType => DateDataField(AstroBinaryFormat.IntDataField(Bytes.SIZEOF_INT, IntegerType))
          case TimestampType => TimestampDataField(AstroBinaryFormat.LongDataField(Bytes.SIZEOF_LONG, LongType))
          case dt: DecimalType => HbaseBinaryFormat.DecimalDataField(dt)
          case at: ArrayType => ArrayDataField(at, encodingFormat, separators, level, hiveStore)
          case st: StructType => StructDataField(st, encodingFormat, separators, level, hiveStore)
          case mt: MapType => MapDataField(mt, encodingFormat, separators, level, hiveStore)
          case StringType => StringDataField(StringType)
        }
      case STRING_FORMAT =>
        dataType match {
          case ByteType => StringFormat.ByteDataField(Bytes.SIZEOF_BYTE, ByteType)
          case BooleanType => StringFormat.BooleanDataField(Bytes.SIZEOF_BOOLEAN, BooleanType)
          case ShortType => StringFormat.ShortDataField(Bytes.SIZEOF_SHORT, ShortType)
          case IntegerType => StringFormat.IntDataField(Bytes.SIZEOF_INT, IntegerType)
          case LongType => StringFormat.LongDataField(Bytes.SIZEOF_LONG, LongType)
          case FloatType => StringFormat.FloatDataField(Bytes.SIZEOF_FLOAT, FloatType)
          case DoubleType => StringFormat.DoubleDataField(Bytes.SIZEOF_DOUBLE, DoubleType)
          case DateType => DateDataField(StringFormat.IntDataField(Bytes.SIZEOF_INT, IntegerType))
          case TimestampType => TimestampDataField(StringFormat.LongDataField(Bytes.SIZEOF_LONG, LongType))
          case dt: DecimalType => StringFormat.DecimalDataField(dt)
          case at: ArrayType => ArrayDataField(at, encodingFormat, separators, level, hiveStore)
          case st: StructType => StructDataField(st, encodingFormat, separators, level, hiveStore)
          case mt: MapType => MapDataField(mt, encodingFormat, separators, level, hiveStore)
          case StringType => StringDataField(StringType)
        }
      case HBASE_FORMAT =>
        dataType match {
          case ByteType => HbaseBinaryFormat.ByteDataField(Bytes.SIZEOF_BYTE, ByteType)
          case BooleanType => HbaseBinaryFormat.BooleanDataField(Bytes.SIZEOF_BOOLEAN, BooleanType)
          case ShortType => HbaseBinaryFormat.ShortDataField(Bytes.SIZEOF_SHORT, ShortType)
          case IntegerType => HbaseBinaryFormat.IntDataField(Bytes.SIZEOF_INT, IntegerType)
          case LongType => HbaseBinaryFormat.LongDataField(Bytes.SIZEOF_LONG, LongType)
          case FloatType => HbaseBinaryFormat.FloatDataField(Bytes.SIZEOF_FLOAT, FloatType)
          case DoubleType => HbaseBinaryFormat.DoubleDataField(Bytes.SIZEOF_DOUBLE, DoubleType)
          case DateType => DateDataField(HbaseBinaryFormat.IntDataField(Bytes.SIZEOF_INT, IntegerType))
          case TimestampType => TimestampDataField(HbaseBinaryFormat.LongDataField(Bytes.SIZEOF_LONG, LongType))
          case dt: DecimalType => HbaseBinaryFormat.DecimalDataField(dt)
          case at: ArrayType => ArrayDataField(at, encodingFormat, separators, level, hiveStore)
          case st: StructType => StructDataField(st, encodingFormat, separators, level, hiveStore)
          case mt: MapType => MapDataField(mt, encodingFormat, separators, level, hiveStore)
          case StringType => StringDataField(StringType)
        }
    }
  }
}

/**
 * It caches the FieldData objects to avoid creating multiple times.
 * @param encodingFormat
 * @param separators
 * @param hiveStorage
 */
class FieldDataCache(encodingFormat: String, separators: Array[Byte], hiveStorage: Boolean) extends Serializable {

  val cache = new HashMap[DataType, FieldData]

  def get(dataType: DataType): FieldData = {
    cache.get(dataType) match {
      case Some(field) => field
      case None =>
        val field = FieldFactory.createFieldData(dataType, encodingFormat, separators, 0, hiveStorage)
        cache += dataType -> field
        field
    }
  }

  def setFields(fieldMap: Map[DataType, FieldData]): Unit = {
    cache ++: fieldMap
  }

}

/**
 * This interface class for FieldData of all datatypes. This class is responsible for all data parsing,encoding and
 * decoding of any particular datatype.
 */
abstract class FieldData extends Serializable with Logging {

  /**
   * Basically it would be datatype internal type
   */
  type InternalType

  /**
   * It returns raw bytes for give value
   * @param value
   * @return
   */
  def getRawBytes(value: InternalType): HBaseRawType

  /**
   * It returns the value after it decoded from raw bytes to value.
   * @param bytes
   * @param offset
   * @param length
   * @return
   */
  def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType

  /**
   * It converts raw bytes to data and set to row.
   * @param row
   * @param index
   * @param src
   * @param offset
   * @param length
   */
  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int)

  /**
   * It retrives data from row and converts it into raw bytes.
   * @param row
   * @param index
   * @return
   */
  def getRowColumnInHBaseRawType(row: InternalRow, index: Int): HBaseRawType = {
    if (row.isNullAt(index)) return new Array[Byte](0)
    getRowColumnInHBaseRawTypeInternal(row, index)
  }

  protected def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType

  /**
   * It parses the String to corresponding datatype value
   * @param input
   * @return
   */
  def parseStringToData(input: String): Any = {
    input match {
      case null => null
      case _ =>
        try {
          parseStringToDataInternal(input)
        } catch {
          case _: NumberFormatException | _: IllegalArgumentException =>
            logWarning(s"$input can't be converted, set this value to be null.")
            null
        }
    }
  }

  protected def parseStringToDataInternal(input: String): InternalType

  def parseStringToTypeDataBytes(input: String): HBaseRawType = {
    input match {
      case null => new Array[Byte](0)
      case "" => new Array[Byte](0)
      case _ =>
        try {
          getRawBytes(parseStringToDataInternal(input))
        } catch {
          case e: NumberFormatException =>
            // If there is an error of dataType cast, we set this field to be null. And we will
            // print the error log outside this function.
            new Array[Byte](0)
        }
    }
  }

  def getDataStorageFormat: String

  protected def getIndex(bytes: HBaseRawType, offset: Int, length: Int, separator: Byte): Int = {
    for (a <- 0 until length) {
      if (bytes(offset + a) == separator) return a
    }
    -1
  }

}

abstract class PrimitiveDataField extends FieldData {

  val length: Int
}

case class StringDataField(dataType: StringType) extends FieldData {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.update(index, getValueFromBytes(src, offset, length))
  }

  def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
    UTF8String.fromBytes(bytes, offset, length)
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(UTF8String.fromString(row.getString(index)))
  }

  def getRawBytes(input: InternalType): HBaseRawType = {
    input.getBytes
  }

  def parseStringToDataInternal(input: String): InternalType = {
    UTF8String.fromString(input)
  }

  def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
}

case class ArrayDataField(dataType: ArrayType,
                          encodingFormat: String,
                          separators: Array[Byte],
                          level: Int,
                          hiveStorage: Boolean) extends FieldData {

  type InternalType = ArrayData

  val separator = separators(0)

  val elementField: FieldData = FieldFactory.createFieldData(dataType.elementType, {
    if (hiveStorage) FieldFactory.STRING_FORMAT else encodingFormat
  }, separators, level + 1)

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.update(index, getValueFromBytes(src, offset, length))
  }

  def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
    var index = getIndex(bytes, offset, length, separator)
    val list = new ArrayBuffer[elementField.InternalType]()
    var offsetLocal = offset
    var lenLocal = length
    while (index > 0) {
      length
      list += elementField.getValueFromBytes(bytes, offsetLocal, index)
      offsetLocal = offsetLocal + index + 1
      lenLocal = lenLocal - (index + 1)
      index = getIndex(bytes, offsetLocal, lenLocal, separator)
    }
    if (index < 0) list += elementField.getValueFromBytes(bytes, offsetLocal, lenLocal)
    new GenericArrayData(list.toArray)
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    val array = row.get(index, dataType)
    array match {
      case a: InternalType => getRawBytes(a)
      case b: mutable.WrappedArray[Any] =>
        val arrayInternal = elementField match {
          case s: StringDataField => b.toArray.map(f => UTF8String.fromString(f.toString).asInstanceOf[Any])
          case _ => b.toArray
        }
        getRawBytes(new GenericArrayData(arrayInternal))
    }
  }

  def getRawBytes(value: InternalType): HBaseRawType = {
    val b = new ByteArrayOutputStream()
    val o = new DataOutputStream(b)
    val numElements: Int = value.numElements()
    value.foreach(dataType.elementType, writeData)
    def writeData(index: Int, data: Any) {
      o.write(elementField.getRawBytes(data.asInstanceOf[elementField.InternalType]))
      if (index < numElements - 1)
        o.write(separator)
    }
    o.close()
    b.toByteArray
  }

  def parseStringToDataInternal(input: String): InternalType = {
    new GenericArrayData(input.split(separator.toChar).map { f =>
      elementField.parseStringToData(f).asInstanceOf[elementField.InternalType]
    })
  }

  def getDataStorageFormat: String = encodingFormat
}

case class StructDataField(dataType: StructType,
                           encodingFormat: String,
                           separators: Array[Byte],
                           level: Int,
                           hiveStorage: Boolean) extends FieldData {

  type InternalType = InternalRow

  val separator = separators(level)

  val fields = dataType.fields.map { sf =>
    FieldFactory.createFieldData(sf.dataType, {
      if (hiveStorage) FieldFactory.STRING_FORMAT else encodingFormat
    }, separators, level + 1)
  }

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.update(index, getValueFromBytes(src, offset, length))
  }

  def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
    var index = getIndex(bytes, offset, length, separator)
    var offsetLocal = offset
    var lenLocal = length
    val row = new GenericMutableRow(fields.size)
    var i = 0
    while (index > 0) {
      row.update(i, fields(i).getValueFromBytes(bytes, offsetLocal, index))
      offsetLocal = offsetLocal + index + 1
      lenLocal = lenLocal - (index + 1)
      index = getIndex(bytes, offsetLocal, lenLocal, separator)
      i = i + 1
    }
    row.update(i, fields(i).getValueFromBytes(bytes, offsetLocal, lenLocal))
    row
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    val struct = row.get(index, dataType)
    struct match {
      case r: InternalType => getRawBytes(r)
      case g: GenericRowWithSchema =>
        val row = new GenericMutableRow(g.size)
        for (i <- 0 until g.size) {
          val field = if (g.get(i).isInstanceOf[String]) UTF8String.fromString(g.get(i).toString) else g.get(i)
          row.update(i, field)
        }
        getRawBytes(row)
    }
  }

  def getRawBytes(value: InternalType): HBaseRawType = {
    val b = new ByteArrayOutputStream()
    val o = new DataOutputStream(b)
    val numElements: Int = value.numFields
    for (a <- 0 until numElements) {
      val f = fields(a)
      o.write(f.getRawBytes(value.get(a, dataType.fields(a).dataType).asInstanceOf[f.InternalType]))
      if (a < numElements - 1)
        o.write(separator)
    }
    o.close()
    b.toByteArray
  }

  def parseStringToDataInternal(input: String): InternalType = {
    val row = new GenericMutableRow(fields.size)
    input.split(separator.toChar).zipWithIndex.map { f =>
      row.update(f._2, fields(f._2).parseStringToData(f._1))
    }
    row
  }

  def getDataStorageFormat: String = encodingFormat
}

case class MapDataField(dataType: MapType,
                        encodingFormat: String,
                        separators: Array[Byte],
                        level: Int,
                        hiveStorage: Boolean) extends FieldData {

  type InternalType = MapData

  val collSeparator = separators(level)

  val keySeparator = separators(level + 1)

  val (keyField, valField) =
    (FieldFactory.createFieldData(dataType.keyType, {
      if (hiveStorage) FieldFactory.STRING_FORMAT else encodingFormat
    }, separators, level + 2),
      FieldFactory.createFieldData(dataType.valueType, {
        if (hiveStorage) FieldFactory.STRING_FORMAT else encodingFormat
      }, separators, level + 2))

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.update(index, getValueFromBytes(src, offset, length))
  }

  def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
    var index = getIndex(bytes, offset, length, collSeparator)
    val keyList = new ArrayBuffer[keyField.InternalType]()
    val valList = new ArrayBuffer[valField.InternalType]()
    var offsetLocal = offset
    var lenLocal = length
    while (index > 0) {
      val keyIndex = getIndex(bytes, offsetLocal, index, keySeparator)
      keyList += keyField.getValueFromBytes(bytes, offsetLocal, keyIndex)
      valList += valField.getValueFromBytes(bytes, offsetLocal + keyIndex + 1, {
        if (index < 0) lenLocal else index - (keyIndex + 1)
      })
      lenLocal = lenLocal - (index + 1)
      offsetLocal = offsetLocal + index + 1
      index = getIndex(bytes, offsetLocal, lenLocal, collSeparator)
    }
    if (index < 0) {
      val keyIndex = getIndex(bytes, offsetLocal, lenLocal, keySeparator)
      keyList += keyField.getValueFromBytes(bytes, offsetLocal, keyIndex)
      lenLocal = lenLocal - (keyIndex + 1)
      valList += valField.getValueFromBytes(bytes, offsetLocal + keyIndex + 1, lenLocal)
    }

    //    HBaseSerializer.deserialize(bytes, offset, length).asInstanceOf[InternalType]
    new ArrayBasedMapData(new GenericArrayData(keyList.toArray), new GenericArrayData(valList.toArray))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    val map = row.get(index, dataType)
    map match {
      case m: InternalType => getRawBytes(m)
      case m: Map[Any, Any] =>
        val keyList = new ArrayBuffer[Any]()
        val valList = new ArrayBuffer[Any]()
        m.map { kv =>
          val k = if (kv._1.isInstanceOf[String]) UTF8String.fromString(kv._1.toString) else kv._1
          keyList += k
          val v = if (kv._2.isInstanceOf[String]) UTF8String.fromString(kv._2.toString) else kv._2
          valList += v
        }
        getRawBytes(new ArrayBasedMapData(new GenericArrayData(keyList.toArray), new GenericArrayData(valList.toArray)))
    }
  }

  def getRawBytes(value: InternalType): HBaseRawType = {
    val b = new ByteArrayOutputStream()
    val o = new DataOutputStream(b)
    val numElements: Int = value.numElements()
    val keyArray = value.keyArray().toArray(dataType.keyType).asInstanceOf[Array[keyField.InternalType]]
    val valueArray = value.valueArray().toArray(dataType.valueType).asInstanceOf[Array[valField.InternalType]]
    for (a <- 0 until numElements) {
      val d =
        o.write(keyField.getRawBytes(keyArray(a)))
      o.write(keySeparator)
      o.write(valField.getRawBytes(valueArray(a)))
      if (a < numElements - 1)
        o.write(collSeparator)
    }
    o.close()
    b.toByteArray
  }

  def parseStringToDataInternal(input: String): InternalType = {
    val data = input.split(collSeparator.toChar).map { f =>
      val entry = f.split(keySeparator.toChar)
      (keyField.parseStringToData(entry(0)), valField.parseStringToData(entry(1)))
    }
    new ArrayBasedMapData(new GenericArrayData(data.map(_._1)), new GenericArrayData(data.map(_._2)))
  }

  def getDataStorageFormat: String = encodingFormat
}

case class DateDataField(field: AbstractIntDataField) extends FieldData {

  type InternalType = field.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    field.setDataInRow(row, index, src, offset, length)
  }

  def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
    field.getValueFromBytes(bytes, offset, length)
  }

  protected def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    val date = row.get(index, DateType)
    date match {
      case d: Integer => field.getRawBytes(d)
      case d: Date => field.getRawBytes(DateTimeUtils.fromJavaDate(d))
    }

  }

  def getRawBytes(value: InternalType): HBaseRawType = {
    field.getRawBytes(value)
  }

  protected def parseStringToDataInternal(input: String): InternalType = {
    DateTimeUtils.stringToDate(UTF8String.fromString(input)).get
  }

  def getDataStorageFormat: String = field.getDataStorageFormat
}

case class TimestampDataField(field: AbstractLongDataField) extends FieldData {

  type InternalType = field.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    field.setDataInRow(row, index, src, offset, length)
  }

  def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
    field.getValueFromBytes(bytes, offset, length)
  }

  protected def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    val date = row.get(index, TimestampType)
    date match {
      case d: java.lang.Long => field.getRawBytes(d)
      case d: Timestamp => field.getRawBytes(DateTimeUtils.fromJavaTimestamp(d))
    }
  }

  def getRawBytes(value: InternalType): HBaseRawType = {
    field.getRawBytes(value)
  }

  protected def parseStringToDataInternal(input: String): InternalType = {
    DateTimeUtils.stringToTimestamp(UTF8String.fromString(input)).get
  }

  def getDataStorageFormat: String = field.getDataStorageFormat
}

abstract class AbstractByteDataField(length: Int, val dataType: ByteType) extends PrimitiveDataField {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) = {
    row.setByte(index, getValueFromBytes(src, offset, length))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(row.getByte(index))
  }

  def parseStringToDataInternal(input: String): InternalType = {
    input.toByte
  }
}

abstract class AbstractBooleanDataField(length: Int, val dataType: BooleanType) extends PrimitiveDataField {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.setBoolean(index, getValueFromBytes(src, offset, length))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(row.getBoolean(index))
  }

  def parseStringToDataInternal(input: String): InternalType = {
    input.toBoolean
  }
}

abstract class AbstractShortDataField(length: Int, val dataType: ShortType) extends PrimitiveDataField {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.setShort(index, getValueFromBytes(src, offset, length))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(row.getShort(index))
  }

  def parseStringToDataInternal(input: String): InternalType = {
    input.toShort
  }
}

abstract class AbstractIntDataField(length: Int, val dataType: IntegerType) extends PrimitiveDataField {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.setInt(index, getValueFromBytes(src, offset, length))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(row.getInt(index))
  }

  def parseStringToDataInternal(input: String): InternalType = {
    input.toInt
  }
}

abstract class AbstractLongDataField(length: Int, val dataType: LongType) extends PrimitiveDataField {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.setLong(index, getValueFromBytes(src, offset, length))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(row.getLong(index))
  }

  def parseStringToDataInternal(input: String): InternalType = {
    input.toLong
  }
}

abstract class AbstractFloatDataField(length: Int, val dataType: FloatType) extends PrimitiveDataField {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.setFloat(index, getValueFromBytes(src, offset, length))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(row.getFloat(index))
  }

  def parseStringToDataInternal(input: String): InternalType = {
    input.toFloat
  }
}

abstract class AbstractDoubleDataField(length: Int, val dataType: DoubleType) extends PrimitiveDataField {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.setDouble(index, getValueFromBytes(src, offset, length))
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    getRawBytes(row.getDouble(index))
  }

  def parseStringToDataInternal(input: String): InternalType = {
    input.toDouble
  }
}

abstract class AbstractDecimalDataField(val dataType: DecimalType) extends FieldData {

  type InternalType = dataType.InternalType

  def setDataInRow(row: MutableRow, index: Int, src: HBaseRawType, offset: Int, length: Int) {
    row.setDecimal(index, getValueFromBytes(src, offset, length), dataType.precision)
  }

  def getRowColumnInHBaseRawTypeInternal(row: InternalRow, index: Int): HBaseRawType = {
    val deci = row.get(index, dataType)
    deci match {
      case d: InternalType => getRawBytes(row.getDecimal(index, dataType.precision, dataType.scale))
      case d: java.math.BigDecimal => getRawBytes(Decimal(d))
    }
  }

  def parseStringToDataInternal(input: String): InternalType = {
    val decimal: Decimal = Decimal(BigDecimal(input))
    //    decimal.changePrecision(dataType.precision, dataType.scale)
    decimal
  }
}


object AstroBinaryFormat {

  case class ByteDataField(length: Int, dt: ByteType) extends AbstractByteDataField(length, dt) {

    def getRawBytes(value: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      buffer(0) = (value ^ 0x80).asInstanceOf[Byte]
      buffer
    }

    def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      val v: Int = bytes(offset) ^ 0x80
      v.asInstanceOf[Byte]
    }

    def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
  }

  case class BooleanDataField(length: Int, dt: BooleanType) extends AbstractBooleanDataField(length, dt) {

    def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      bytes(offset) != 0
    }

    def getRawBytes(value: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      if (value) {
        buffer(0) = (-1).asInstanceOf[Byte]
      } else {
        buffer(0) = 0.asInstanceOf[Byte]
      }
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
  }

  case class ShortDataField(length: Int, dt: ShortType) extends AbstractShortDataField(length, dt) {

    def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      // flip sign bit back
      var v: Int = bytes(offset) ^ 0x80
      v = (v << 8) + (bytes(1 + offset) & 0xff)
      v.asInstanceOf[Short]
    }

    def getRawBytes(value: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      buffer(0) = ((value >> 8) ^ 0x80).asInstanceOf[Byte]
      buffer(1) = value.asInstanceOf[Byte]
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
  }

  case class IntDataField(length: Int, dt: IntegerType) extends AbstractIntDataField(length, dt) {

    def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      // Flip sign bit back
      var v: Int = bytes(offset) ^ 0x80
      for (i <- 1 to Bytes.SIZEOF_INT - 1) {
        v = (v << 8) + (bytes(i + offset) & 0xff)
      }
      v
    }

    def getRawBytes(value: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      // Flip sign bit so that INTEGER is binary comparable
      buffer(0) = ((value >> 24) ^ 0x80).asInstanceOf[Byte]
      buffer(1) = (value >> 16).asInstanceOf[Byte]
      buffer(2) = (value >> 8).asInstanceOf[Byte]
      buffer(3) = value.asInstanceOf[Byte]
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
  }

  case class LongDataField(length: Int, dt: LongType) extends AbstractLongDataField(length, dt) {

    def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      // Flip sign bit back
      var v: Long = bytes(offset) ^ 0x80
      for (i <- 1 to Bytes.SIZEOF_LONG - 1) {
        v = (v << 8) + (bytes(i + offset) & 0xff)
      }
      v
    }

    def getRawBytes(input: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      // Flip sign bit so that INTEGER is binary comparable
      buffer(0) = ((input >> 56) ^ 0x80).asInstanceOf[Byte]
      buffer(1) = (input >> 48).asInstanceOf[Byte]
      buffer(2) = (input >> 40).asInstanceOf[Byte]
      buffer(3) = (input >> 32).asInstanceOf[Byte]
      buffer(4) = (input >> 24).asInstanceOf[Byte]
      buffer(5) = (input >> 16).asInstanceOf[Byte]
      buffer(6) = (input >> 8).asInstanceOf[Byte]
      buffer(7) = input.asInstanceOf[Byte]
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
  }

  case class FloatDataField(length: Int, dt: FloatType) extends AbstractFloatDataField(length, dt) {

    def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      var i = Bytes.toInt(bytes, offset)
      i = i - 1
      i ^= (~i >> Integer.SIZE - 1) | Integer.MIN_VALUE
      java.lang.Float.intBitsToFloat(i)
    }

    def getRawBytes(input: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      var i: Int = java.lang.Float.floatToIntBits(input)
      i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1
      Bytes.putInt(buffer, 0, i)
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
  }

  case class DoubleDataField(length: Int, dt: DoubleType) extends AbstractDoubleDataField(length, dt) {

    def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      var l: Long = Bytes.toLong(bytes, offset, Bytes.SIZEOF_DOUBLE)
      l = l - 1
      l ^= (~l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE
      java.lang.Double.longBitsToDouble(l)
    }

    def getRawBytes(input: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      var l: Long = java.lang.Double.doubleToLongBits(input)
      l = (l ^ ((l >> java.lang.Long.SIZE - 1) | java.lang.Long.MIN_VALUE)) + 1
      Bytes.putLong(buffer, 0, l)
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.BINARY_FORMAT
  }

}

object StringFormat {

  trait StringFieldBase {

    def toString(input: HBaseRawType, offset: Int, length: Int): String = {
      toUTF8String(input, offset, length).toString
    }

    def toUTF8String(input: HBaseRawType, offset: Int, length: Int): UTF8String = {
      UTF8String.fromBytes(input, offset, length)
    }

    def getDataStorageFormat: String = FieldFactory.STRING_FORMAT
  }

  case class ByteDataField(length: Int, dt: ByteType) extends AbstractByteDataField(length, dt) with StringFieldBase {

    override def getRawBytes(value: InternalType): HBaseRawType = {
      value.toString.getBytes
    }

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      toString(bytes, offset, length).toByte
    }
  }

  case class BooleanDataField(length: Int, dt: BooleanType)
    extends AbstractBooleanDataField(length, dt) with StringFieldBase {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      toString(bytes, offset, length).toBoolean
    }

    override def getRawBytes(value: InternalType): HBaseRawType = {
      value.toString.getBytes
    }
  }

  case class ShortDataField(length: Int, dt: ShortType)
    extends AbstractShortDataField(length, dt) with StringFieldBase {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      toString(bytes, offset, length).toShort
    }

    override def getRawBytes(value: InternalType): HBaseRawType = {
      value.toString.getBytes
    }
  }

  case class IntDataField(length: Int, dt: IntegerType)
    extends AbstractIntDataField(length, dt) with StringFieldBase {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      toString(bytes, offset, length).toInt
    }

    override def getRawBytes(value: InternalType): HBaseRawType = {
      value.toString.getBytes
    }
  }

  case class LongDataField(length: Int, dt: LongType)
    extends AbstractLongDataField(length, dt) with StringFieldBase {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      toString(bytes, offset, length).toLong
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      input.toString.getBytes
    }
  }

  case class FloatDataField(length: Int, dt: FloatType)
    extends AbstractFloatDataField(length, dt) with StringFieldBase {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      toString(bytes, offset, length).toFloat
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      input.toString.getBytes
    }
  }

  case class DoubleDataField(length: Int, dt: DoubleType)
    extends AbstractDoubleDataField(length, dt) with StringFieldBase {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      toString(bytes, offset, length).toDouble
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      input.toString.getBytes
    }
  }

  case class DecimalDataField(dt: DecimalType)
    extends AbstractDecimalDataField(dt) with StringFieldBase {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      Decimal(BigDecimal(toString(bytes, offset, length)))
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      input.toBigDecimal.toString.getBytes
    }
  }

}

object HbaseBinaryFormat {

  case class ByteDataField(length: Int, dt: ByteType) extends AbstractByteDataField(length, dt) {

    override def getRawBytes(value: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      buffer(0) = value
      buffer
    }

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      val v: Int = bytes(offset)
      v.asInstanceOf[Byte]
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

  case class BooleanDataField(length: Int, dt: BooleanType) extends AbstractBooleanDataField(length, dt) {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      bytes(offset) != 0
    }

    override def getRawBytes(value: InternalType): HBaseRawType = {
      Bytes.toBytes(value)
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

  case class ShortDataField(length: Int, dt: ShortType) extends AbstractShortDataField(length, dt) {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      Bytes.toShort(bytes, offset)
    }

    override def getRawBytes(value: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      Bytes.putShort(buffer, 0, value)
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

  case class IntDataField(length: Int, dt: IntegerType) extends AbstractIntDataField(length, dt) {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      Bytes.toInt(bytes, offset)
    }

    override def getRawBytes(value: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      Bytes.putInt(buffer, 0, value)
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

  case class LongDataField(length: Int, dt: LongType) extends AbstractLongDataField(length, dt) {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      Bytes.toLong(bytes, offset)
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      Bytes.putLong(buffer, 0, input)
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

  case class FloatDataField(length: Int, dt: FloatType) extends AbstractFloatDataField(length, dt) {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      Bytes.toFloat(bytes, offset)
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      Bytes.putFloat(buffer, 0, input)
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

  case class DoubleDataField(length: Int, dt: DoubleType) extends AbstractDoubleDataField(length, dt) {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      Bytes.toDouble(bytes, offset)
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      val buffer = new HBaseRawType(length)
      Bytes.putDouble(buffer, 0, input)
      buffer
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

  case class DecimalDataField(dt: DecimalType)
    extends AbstractDecimalDataField(dt) {

    override def getValueFromBytes(bytes: HBaseRawType, offset: Int, length: Int): InternalType = {
      Decimal(Bytes.toBigDecimal(bytes, offset, length))
    }

    override def getRawBytes(input: InternalType): HBaseRawType = {
      Bytes.toBytes(input.toJavaBigDecimal)
    }

    def getDataStorageFormat: String = FieldFactory.HBASE_FORMAT
  }

}


