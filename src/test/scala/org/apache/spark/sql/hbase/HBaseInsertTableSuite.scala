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

import org.apache.spark.sql.Row

class HBaseInsertTableSuite extends TestBaseWithNonSplitData {

  var testnm = "Insert all rows to the table from other table"
  test("Insert all rows to the table from other table") {
    dropLogicalTable("insertTestTable")
    val createQuery =
      s"""CREATE TABLE insertTestTable(
         |  strcol STRING,
         |  bytecol TINYINT,
         |  shortcol SMALLINT,
         |  intcol INTEGER,
         |  longcol LONG,
         |  floatcol FLOAT,
         |  doublecol DOUBLE
         |)
         |USING org.apache.spark.sql.hbase.HBaseSource
         |OPTIONS(
         |  tableName "insertTestTable",
         |  hbaseTableName "hinsertTestTable",
         |  keyCols "doublecol, strcol, intcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol"
         |)"""
        .stripMargin
    runSql(createQuery)

    val insertQuery =
      s"""INSERT INTO TABLE insertTestTable SELECT * FROM $TestTableName"""
        .stripMargin
    runSql(insertQuery)

    val testQuery = "SELECT * FROM insertTestTable"
    val testResult = runSql(testQuery)
    val targetResult = runSql(s"SELECT * FROM $TestTableName")
    assert(testResult.length == targetResult.length, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runSql("DROP TABLE insertTestTable")
  }

  testnm = "Insert few rows to the table from other table after applying filter"
  test("Insert few rows to the table from other table after applying filter") {
    val createQuery =
      s"""CREATE TABLE insertTestTableFilter(
         |  strcol STRING,
         |  bytecol TINYINT,
         |  shortcol SMALLINT,
         |  intcol INTEGER,
         |  longcol LONG,
         |  floatcol FLOAT,
         |  doublecol DOUBLE
         |)
         |USING org.apache.spark.sql.hbase.HBaseSource
         |OPTIONS(
         |  tableName "insertTestTableFilter",
         |  hbaseTableName "hinsertTestTableFilter",
         |  keyCols "doublecol, strcol, intcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol"
         |)"""
        .stripMargin
    runSql(createQuery)

    val insertQuery =
      s"""insert into table insertTestTableFilter select * from $TestTableName
        where doublecol > 5678912.345681"""
        .stripMargin
    runSql(insertQuery)

    val testQuery = "select * from insertTestTableFilter"
    val testResult = runSql(testQuery)
    val targetResult = runSql(s"select * from $TestTableName where doublecol > 5678912.345681")
    assert(testResult.length == targetResult.length, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runSql("Drop Table insertTestTableFilter")
  }

  def compareResults(fetchResult: Array[Row], targetResult: Array[Row]) = {
    val res = {
      for (rx <- targetResult.indices)
      yield compareWithTol(fetchResult(rx).toSeq, targetResult(rx).toSeq, s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")
  }

  testnm = "Insert few columns to the table from other table"
  test("Insert few columns to the table from other table") {
    dropLogicalTable("insertTestTableFewCols")
    val createQuery =
      s"""CREATE TABLE insertTestTableFewCols(
         |  strcol STRING,
         |  bytecol TINYINT,
         |  shortcol SMALLINT,
         |  intcol INTEGER
         |)
         |USING org.apache.spark.sql.hbase.HBaseSource
         |OPTIONS(
         |  tableName "insertTestTableFewCols",
         |  hbaseTableName "hinsertTestTableFewCols",
         |  keyCols "strcol, intcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol"
         |)"""
        .stripMargin
    runSql(createQuery)

    val insertQuery =
      s"""INSERT INTO TABLE insertTestTableFewCols SELECT strcol, bytecol,
        shortcol, intcol FROM $TestTableName ORDER BY strcol"""
        .stripMargin
    runSql(insertQuery)

    val testQuery =
      "SELECT strcol, bytecol, shortcol, intcol FROM insertTestTableFewCols ORDER BY strcol"
    val testResult = runSql(testQuery)
    val targetResult =
      runSql(s"SELECT strcol, bytecol, shortcol, intcol FROM $TestTableName ORDER BY strcol")
    assert(testResult.length == targetResult.length, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    dropLogicalTable("insertTestTableFewCols")
  }

  testnm = "Insert into values test"
  test("Insert into values test") {
    dropLogicalTable("insertValuesTest")
    val createQuery =
      s"""CREATE TABLE insertValuesTest(
         |  strcol STRING,
         |  bytecol TINYINT,
         |  shortcol SMALLINT,
         |  intcol INTEGER
         |)
         |USING org.apache.spark.sql.hbase.HBaseSource
         |OPTIONS(
         |  tableName "insertValuesTest",
         |  hbaseTableName "hinsertValuesTest",
         |  keyCols "strcol, intcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol"
         |)"""
        .stripMargin
    runSql(createQuery)

    val insertQuery1 = s"INSERT INTO TABLE insertValuesTest VALUES('Row0','a',12340,23456780)"
    val insertQuery2 = s"INSERT INTO TABLE insertValuesTest VALUES('Row1','b',12345,23456789)"
    val insertQuery3 = s"INSERT INTO TABLE insertValuesTest VALUES('Row2','c',12342,23456782)"
    runSql(insertQuery1)
    runSql(insertQuery2)
    runSql(insertQuery3)

    val testQuery = "SELECT * FROM insertValuesTest ORDER BY strcol"
    val testResult = runSql(testQuery)
    assert(testResult.length == 3, s"$testnm failed on size")

    val exparr = Array(Array("Row0", null, 12340, 23456780),
      Array("Row1", null, 12345, 23456789),
      Array("Row2", null, 12342, 23456782))

    val res = {
      for (rx <- 0 until 3)
      yield compareWithTol(testResult(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    runSql("DROP TABLE insertValuesTest")
  }

  testnm = "Insert nullable values test"
  test("Insert nullable values test") {
    dropLogicalTable("insertNullValuesTest")
    val createQuery =
      s"""CREATE TABLE insertNullValuesTest(
         |  strcol STRING,
         |  bytecol TINYINT,
         |  shortcol SMALLINT,
         |  intcol INTEGER
         |)
         |USING org.apache.spark.sql.hbase.HBaseSource
         |OPTIONS(
         |  tableName "insertNullValuesTest",
         |  hbaseTableName "hinsertNullValuesTest",
         |  keyCols "strcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol, intcol=cf1.hintcol"
         |)"""
        .stripMargin
    runSql(createQuery)

    val insertQuery0 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row0', null,  12340, 23456780, 'abc')"
    val insertQuery1 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row1', null,  12341, 23456781)"
    val insertQuery2 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row2', 'b',   null, 23456789)"
    val insertQuery3 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row3', 'c',  12342)"
    runSql(insertQuery0)
    runSql(insertQuery1)
    runSql(insertQuery2)
    runSql(insertQuery3)

    val selectAllQuery = "SELECT * FROM insertNullValuesTest ORDER BY strcol"
    val selectAllResult = runSql(selectAllQuery)

    assert(selectAllResult.length == 4, s"$testnm failed on size")

    var currentResultRow: Int = 0

    // check 1st result row
    assert(selectAllResult(currentResultRow).length == 4, s"$testnm failed on row size (# of cols)")
    assert(selectAllResult(currentResultRow)(0) === s"Row0", s"$testnm failed on returned Row0")
    assert(selectAllResult(currentResultRow)(1) == null, s"$testnm failed on returned null")
    assert(selectAllResult(currentResultRow)(2) == 12340, s"$testnm failed on returned 12340")
    assert(selectAllResult(currentResultRow)(3) == 23456780, s"$testnm failed on returned 23456780")

    currentResultRow += 1

    // check 2nd result row
    assert(selectAllResult(currentResultRow).length == 4, s"$testnm failed on row size (# of cols)")
    assert(selectAllResult(currentResultRow)(0) === s"Row1", s"$testnm failed on returned Row1")
    assert(selectAllResult(currentResultRow)(1) == null, s"$testnm failed on returned null")
    assert(selectAllResult(currentResultRow)(2) == 12341, s"$testnm failed on returned 12341")
    assert(selectAllResult(currentResultRow)(3) == 23456781, s"$testnm failed on returned 23456781")

    currentResultRow += 1

    // check 3rd result row
    assert(selectAllResult(currentResultRow).length == 4, s"$testnm failed on row size (# of cols)")
    assert(selectAllResult(currentResultRow)(0) === s"Row2", s"$testnm failed on returned Row3")
    assert(selectAllResult(currentResultRow)(1) == null, s"$testnm failed on returned b")
    assert(selectAllResult(currentResultRow)(2) == null, s"$testnm failed on returned null")
    assert(selectAllResult(currentResultRow)(3) == 23456789, s"$testnm failed on returned 23456789")

    currentResultRow += 1

    // check 4th result row
    assert(selectAllResult(currentResultRow).length == 4, s"$testnm failed on row size (# of cols)")
    assert(selectAllResult(currentResultRow)(0) === s"Row3", s"$testnm failed on returned Row4")
    assert(selectAllResult(currentResultRow)(1) == null, s"$testnm failed on returned c")
    assert(selectAllResult(currentResultRow)(2) == 12342, s"$testnm failed on returned 12342")
    assert(selectAllResult(currentResultRow)(3) == null, s"$testnm failed on returned null")

    // test 'where col is not null'

    val selectWhereIsNotNullQuery = "SELECT * FROM insertNullValuesTest WHERE intcol IS NOT NULL ORDER BY strcol"
    val selectWhereIsNotNullResult = runSql(selectWhereIsNotNullQuery)
    assert(selectWhereIsNotNullResult.length == 3, s"$testnm failed on size")

    val insertQuery4 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row4', 'd',  'a')"
    runSql(insertQuery4)
    val selectRow4Query = "SELECT * FROM insertNullValuesTest where strcol = 'Row4'"
    val row4Result = runSql(selectRow4Query)
    assert(row4Result(0)(3) == null, "data type error should return null in no-rowkey")

    dropLogicalTable("insertNullValuesTest")
  }

}
