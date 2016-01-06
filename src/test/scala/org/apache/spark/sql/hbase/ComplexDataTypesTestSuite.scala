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

import java.sql.{Timestamp, Date}

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

import scala.collection.mutable

/**
 * Created by root1 on 1/12/15.
 */
class ComplexDataTypesTestSuite extends TestBaseWithNonSplitData {

  val TestTableDataTypesName = "TestTableDataTypesName"
  val TestHBaseTableDataTypesName: String = s"Hb$TestTableDataTypesName"
  val TestTableDataTypesSelectName = "TestTableDataTypesSelectName"
  val TestHBaseTableDataTypesSelectName: String = s"Hb$TestTableDataTypesSelectName"
  override val DefaultLoadFile = "testTableDataTypes.txt"
  var testnm = ""

  override protected def beforeAll() = {
    //    super.beforeAll()
    val testTableCreationHiveSQL =
      s"""CREATE TABLE $TestTableDataTypesName(
         |  strcol STRING,
         |  bytecol TINYINT,
         |  shortcol SMALLINT,
         |  intcol INTEGER,
         |  longcol LONG,
         |  floatcol FLOAT,
         |  doublecol DOUBLE,
         |  datecol DATE,
         |  timestampcol TIMESTAMP,
         |  arraycol ARRAY<INTEGER>,
         |  structcol STRUCT<col1:INTEGER,col2:STRING,col3:LONG>,
         |  mapcol MAP<STRING,INTEGER>,
         |  decicol DECIMAL
         |)
         |USING org.apache.spark.sql.hbase.HBaseSource
         |OPTIONS(
         |  hbaseTableName "$TestHBaseTableDataTypesName",
         |  keyCols "doublecol, strcol, intcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol,datecol=cf2.hdatecol,timestampcol=cf2.htimestampcol,arraycol=cf2.harraycol,structcol=cf2.hstructcol,mapcol=cf2.hmapcol,decicol=cf2.hdecicol",
         |  collectionSeperator "~",
         |  mapkeySeperator "&"
         |)"""
        .stripMargin
    createTable(TestTableDataTypesName, TestHBaseTableDataTypesName, testTableCreationHiveSQL)
    loadData(TestTableDataTypesName, s"$CsvPath/$DefaultLoadFile")

    val testTableCreationHiveSQL1 =
      s"""CREATE TABLE $TestTableDataTypesSelectName(
         |  strcol STRING,
         |  bytecol TINYINT,
         |  shortcol SMALLINT,
         |  intcol INTEGER,
         |  longcol LONG,
         |  floatcol FLOAT,
         |  doublecol DOUBLE,
         |  datecol DATE,
         |  timestampcol TIMESTAMP,
         |  arraycol ARRAY<INTEGER>,
         |  structcol STRUCT<col1:INTEGER,col2:STRING,col3:LONG>,
         |  mapcol MAP<STRING,INTEGER>,
         |  decicol DECIMAL
         |)
         |USING org.apache.spark.sql.hbase.HBaseSource
         |OPTIONS(
         |  hbaseTableName "$TestHBaseTableDataTypesSelectName",
         |  keyCols "doublecol, strcol, intcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol,datecol=cf2.hdatecol,timestampcol=cf2.htimestampcol,arraycol=cf2.harraycol,structcol=cf2.hstructcol,mapcol=cf2.hmapcol,decicol=cf2.hdecicol",
         |  collectionSeperator "~",
         |  mapkeySeperator "&"
         |)"""
        .stripMargin
    createTable(TestTableDataTypesSelectName, TestHBaseTableDataTypesSelectName, testTableCreationHiveSQL1)

  }

  override protected def afterAll() = {
//    super.afterAll()
    runSql("DROP TABLE " + TestTableDataTypesName)
    runSql("DROP TABLE " + TestTableDataTypesSelectName)
  }

  testnm = "Select date and timestamp with filter"
  test("Select date and timestamp with filter") {
    val query1 =
      s"""SELECT strcol,datecol,timestampcol  FROM $TestTableDataTypesName
         |WHERE datecol < '2015-01-05' and timestampcol > '2015-01-01 01:01:01' LIMIT 2"""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 2, s"$testnm failed on size")
    val exparr = Array(
      Array("Row4", Date.valueOf("2015-01-02"), Timestamp.valueOf("2015-01-02 01:02:02")),
      Array("Row5", Date.valueOf("2015-01-03"), Timestamp.valueOf("2015-01-03 01:03:03")))

    val res = {
      for (rx <- 0 until 2)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select array datatype"
  test("Select array datatype") {
    val query1 =
      s"""SELECT strcol,arraycol  FROM $TestTableDataTypesName order by strcol LIMIT 2"""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 2, s"$testnm failed on size")
    val exparr = Array(
      Array("Row0", Seq(1,2,3).map(new Integer(_))),
      Array("Row1", Seq(1,2,3).map(new Integer(_))))

    val res = {
      for (rx <- 0 until 2)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select array datatype with filter"
  test("Select array datatype with filter") {
    val query1 =
      s"""SELECT strcol,arraycol  FROM $TestTableDataTypesName  where arraycol[0]=7"""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 1, s"$testnm failed on size")
    val exparr = Array(
      Array("Row5", Seq(7,8,9).map(new Integer(_))))

    val res = {
      for (rx <- 0 until result1.length)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select struct datatype"
  test("Select struct datatype") {
    val query1 =
      s"""SELECT strcol,structcol  FROM $TestTableDataTypesName order by strcol LIMIT 2"""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 2, s"$testnm failed on size")
    val exparr = Array(
      Array("Row0", Seq(1,"2",3)),
      Array("Row1", Seq(1,"2",3)))

    val res = {
      for (rx <- 0 until 2)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select struct datatype with filter"
  test("Select struct datatype with filter") {
    val query1 =
      s"""SELECT strcol,structcol  FROM $TestTableDataTypesName where structcol.col1=7 order by strcol LIMIT 2"""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 1, s"$testnm failed on size")
    val exparr = Array(
      Array("Row5", Seq(7,"8",9)))

    val res = {
      for (rx <- 0 until result1.length)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select map datatype with filter"
  test("Select map datatype with filter") {
    val query1 =
      s"""SELECT strcol,mapcol FROM $TestTableDataTypesName where mapcol["7"]=8 order by strcol LIMIT 2"""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 1, s"$testnm failed on size")
    val exparr = Array(
      Array("Row5", Seq(("7",8),("9",10),("11",12)).toMap))

    val res = {
      for (rx <- 0 until result1.length)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select decimal datatype with filter"
  test("Select decimal datatype with filter") {
    val query1 =
      s"""SELECT strcol,decicol FROM $TestTableDataTypesName where decicol=21221.34345 order by strcol LIMIT 2"""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 1, s"$testnm failed on size")
    val exparr = Array(
      Array("Row5", java.math.BigDecimal.valueOf(21221.34345d)))

    val res = {
      for (rx <- 0 until result1.length)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Insert into complex datatypes"
  test("Insert into complex datatypes") {
    val query =
      s"""INSERT INTO TABLE $TestTableDataTypesName VALUES("Row12",'b',12342,23456782,3456789012342,45657.82, 5678912.345682,"2016-01-01","2016-01-01 01:01:01.0","1~2~3","1~2~3","1&2~3&4~5&6",21221.34343412)"""
        .stripMargin

    runSql(query)

    val query1 =
      s"""SELECT strcol,arraycol  FROM $TestTableDataTypesName  where strcol="Row12""""
        .stripMargin

    val result1 = runSql(query1)
    logInfo(s"$query1 came back with $result1.length results")
    assert(result1.length == 1, s"$testnm failed on size")
    val exparr = Array(
      Array("Row12", Seq(1,2,3).map(new Integer(_))))

    val res = {
      for (rx <- 0 until result1.length)
        yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Insert into complex datatypes table from select "
  test("Insert into complex datatypes table from select") {
    val query1 =
      s"""INSERT INTO TABLE $TestTableDataTypesSelectName select * from $TestTableDataTypesName"""
        .stripMargin

    runSql(query1)

    val query2 =
      s"""SELECT strcol,structcol  FROM $TestTableDataTypesSelectName where structcol.col1=7 order by strcol LIMIT 2"""
        .stripMargin

    val result2 = runSql(query2)
    logInfo(s"$query2 came back with $result2.length results")
    assert(result2.length == 1, s"$testnm failed on size")
    val exparr = Array(
      Array("Row5", Seq(7,"8",9)))

    val res = {
      for (rx <- 0 until result2.length)
        yield compareWithTol(result2(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    logInfo(result2.mkString)
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

}
