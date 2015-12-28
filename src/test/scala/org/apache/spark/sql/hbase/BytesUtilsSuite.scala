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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.hbase.types.HBaseBytesType
import org.apache.spark.sql.hbase.util.HBaseKVHelper
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class BytesUtilsSuite extends FunSuite with BeforeAndAfterAll with Logging {
  test("Bytes Ordering Test") {
    val s = Seq(-257, -256, -255, -129, -128, -127, -64, -16, -4, -1,
      0, 1, 4, 16, 64, 127, 128, 129, 255, 256, 257)
    val result = s.map(i => {
      val fieldData: FieldData = FieldFactory.createFieldData(IntegerType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      (i, fieldData.getRawBytes(i.asInstanceOf[fieldData.InternalType]))
    })
      .sortWith((f, s) =>
        HBaseBytesType.ordering.gt(
          f._2.asInstanceOf[HBaseBytesType.InternalType],
          s._2.asInstanceOf[HBaseBytesType.InternalType]))
    assert(result.map(a => a._1) == s.sorted.reverse)
  }

  def compare(a: Array[Byte], b: Array[Byte]): Int = {
    val length = Math.min(a.length, b.length)
    var result: Int = 0
    for (i <- 0 to length - 1) {
      val diff: Int = (a(i) & 0xff).asInstanceOf[Byte] - (b(i) & 0xff).asInstanceOf[Byte]
      if (diff != 0) {
        result = diff
      }
    }
    result
  }

  test("Bytes Utility Test") {
    val fieldDataBool = FieldFactory.createFieldData(BooleanType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]
    val fieldDataDouble = FieldFactory.createFieldData(DoubleType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]
    val fieldDataFloat = FieldFactory.createFieldData(FloatType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]
    val fieldDataInteger = FieldFactory.createFieldData(IntegerType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]
    val fieldDataLong = FieldFactory.createFieldData(LongType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]
    val fieldDataShort = FieldFactory.createFieldData(ShortType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]
    val fieldDataString = FieldFactory.createFieldData(StringType, FieldFactory.BINARY_FORMAT, Array[Byte]())
    val fieldDataByte = FieldFactory.createFieldData(ByteType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]

    assert(fieldDataBool.getValueFromBytes(
      fieldDataBool.getRawBytes(true.asInstanceOf[fieldDataBool.InternalType]), 0, fieldDataBool.length)
      === true)
    assert(fieldDataBool.getValueFromBytes(
      fieldDataBool.getRawBytes(false.asInstanceOf[fieldDataBool.InternalType]), 0, fieldDataBool.length) === false)

    assert(fieldDataDouble.getValueFromBytes(
      fieldDataDouble.getRawBytes((-12.34d).asInstanceOf[fieldDataDouble.InternalType]), 0, fieldDataDouble.length)
      === -12.34d)
    assert(fieldDataDouble.getValueFromBytes(
      fieldDataDouble.getRawBytes(12.34d.asInstanceOf[fieldDataDouble.InternalType]), 0, fieldDataDouble.length)
      === 12.34d)

    assert(fieldDataFloat.getValueFromBytes(
      fieldDataFloat.getRawBytes(12.34f.asInstanceOf[fieldDataFloat.InternalType]), 0, fieldDataFloat.length)
      === 12.34f)
    assert(fieldDataFloat.getValueFromBytes(
      fieldDataFloat.getRawBytes((-12.34f).asInstanceOf[fieldDataFloat.InternalType]), 0, fieldDataFloat.length)
      === -12.34f)

    assert(fieldDataInteger.getValueFromBytes(
      fieldDataInteger.getRawBytes(12.asInstanceOf[fieldDataInteger.InternalType]), 0, fieldDataInteger.length)
      === 12)
    assert(fieldDataInteger.getValueFromBytes(
      fieldDataInteger.getRawBytes((-12).asInstanceOf[fieldDataInteger.InternalType]), 0, fieldDataInteger.length)
      === -12)

    assert(fieldDataLong.getValueFromBytes(
      fieldDataLong.getRawBytes((1234l).asInstanceOf[fieldDataLong.InternalType]), 0, fieldDataLong.length)
      === 1234l)
    assert(fieldDataLong.getValueFromBytes(
      fieldDataLong.getRawBytes((-1234l).asInstanceOf[fieldDataLong.InternalType]), 0, fieldDataLong.length) === -1234l)

    assert(fieldDataShort.getValueFromBytes(
      fieldDataShort.getRawBytes((12.toShort).asInstanceOf[fieldDataShort.InternalType]), 0, fieldDataShort.length)
      === 12)
    assert(fieldDataShort.getValueFromBytes(
      fieldDataShort.getRawBytes((-12.toShort).asInstanceOf[fieldDataShort.InternalType]), 0, fieldDataShort.length)
      === -12)

    assert(fieldDataString.getValueFromBytes(
      fieldDataString.getRawBytes(UTF8String.fromString("abc").asInstanceOf[fieldDataString.InternalType]), 0, 3)
      === UTF8String.fromString("abc"))
    assert(fieldDataString.getValueFromBytes(
      fieldDataString.getRawBytes(UTF8String.fromString("").asInstanceOf[fieldDataString.InternalType]), 0, 0)
      === UTF8String.fromString(""))

    assert(fieldDataByte.getValueFromBytes(
      fieldDataByte.getRawBytes((5.toByte).asInstanceOf[fieldDataByte.InternalType]), 0, fieldDataByte.length) === 5)
    assert(fieldDataByte.getValueFromBytes(
      fieldDataByte.getRawBytes((-5.toByte).asInstanceOf[fieldDataByte.InternalType]), 0, fieldDataByte.length) === -5)

    assert(compare(fieldDataInteger.getRawBytes(128.asInstanceOf[fieldDataInteger.InternalType]),
      fieldDataInteger.getRawBytes((-128).asInstanceOf[fieldDataInteger.InternalType])) > 0)
  }

  test("byte array plus one") {
    var byteArray = Array[Byte](0x01.toByte, 127.toByte)
    assert(Bytes.compareTo(HBaseKVHelper.addOne(byteArray), Array[Byte](0x01.toByte, 0x80.toByte)) == 0)

    byteArray = Array[Byte](0xff.toByte, 0xff.toByte)
    assert(HBaseKVHelper.addOne(byteArray) == null)

    byteArray = Array[Byte](0x02.toByte, 0xff.toByte)
    assert(Bytes.compareTo(HBaseKVHelper.addOne(byteArray), Array[Byte](0x03.toByte, 0x00.toByte)) == 0)
  }

  test("float comparison") {
    val fieldDataFloat = FieldFactory.createFieldData(FloatType, FieldFactory.BINARY_FORMAT, Array[Byte]())
      .asInstanceOf[PrimitiveDataField]
    val f1 = fieldDataFloat.getRawBytes((-1.23f).asInstanceOf[fieldDataFloat.InternalType])
    val f2 = fieldDataFloat.getRawBytes(100f.asInstanceOf[fieldDataFloat.InternalType])
    assert(Bytes.compareTo(f1, f2) < 0)
  }
}
