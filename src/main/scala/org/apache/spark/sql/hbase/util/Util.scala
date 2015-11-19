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

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.{DeflaterOutputStream, InflaterInputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Util {
  val iteration = new AtomicInteger(0)

  def getTempFilePath(conf: Configuration, prefix: String): String = {
    val fileSystem = FileSystem.get(conf)
    val path = new Path(s"$prefix-${System.currentTimeMillis()}-${iteration.getAndIncrement}")
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
    path.getName
  }

  def serializeHBaseConfiguration(configuration: Configuration): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val deflaterOutputStream = new DeflaterOutputStream(bos)
    val dos = new DataOutputStream(deflaterOutputStream)
    configuration.write(dos)
    dos.close()
    bos.toByteArray
  }

  def deserializeHBaseConfiguration(arr: Array[Byte]) = {
    val conf = HBaseConfiguration.create
    conf.readFields(new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(arr))))
    conf
  }

  def getValueFromString(value: String, dt: DataType): Any = {
    dt match {
      case StringType => UTF8String.fromString(value)
      case ByteType => value.toByte
      case ShortType => value.toShort
      case IntegerType => value.toInt
      case LongType => value.toLong
      case FloatType => value.toFloat
      case DoubleType => value.toDouble
      case BooleanType => value.toBoolean
      case _ => throw new IllegalArgumentException(s"Unrecognized data type: $dt")
    }
  }
}
