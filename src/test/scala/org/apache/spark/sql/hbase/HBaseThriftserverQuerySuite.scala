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

import java.sql.{DriverManager, ResultSet, Statement}

import org.apache.hadoop.hbase.{TableExistsException, TableName}
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

class HBaseThriftserverQuerySuite extends TestBaseWithNonSplitData {

  val TestHBaseTableHiveName = "TestTableName"
  val TestHBaseTablePreLoadName: String = s"Hb$TestHBaseTableHiveName"
  val driverName = "org.apache.hive.jdbc.HiveDriver"
  var testnm = "StarOperator * with limit"
  var stmt: Statement = null

  def getData(resultSet: ResultSet): Seq[Any] = {
    if (resultSet.next()) {
      val count: Int = resultSet.getMetaData.getColumnCount
      val array = new Array[Any](count)
      for (a <- 1 to count) {
        array(a - 1) = resultSet.getObject(a)
      }
      return array.toSeq
    }
    null
  }

  override protected def beforeAll() = {
    val thread = new Thread {
      override def run {
        HiveThriftServer2.startWithContext(TestHbase)
      }
    }
    thread.start
    try {
      Class.forName(driverName);
    } catch {
      case e: ClassNotFoundException =>
        logError("JDBC Driver not loaded", e)
    }
    Thread.sleep(20000)
    val con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", System.getProperty("user.name"), "");
    stmt = con.createStatement();

    val testTableCreationHiveSQL =
      s"""CREATE TABLE $TestHBaseTableHiveName(
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
         |  hbaseTableName "$TestHBaseTablePreLoadName",
         |  keyCols "doublecol, strcol, intcol",
         |  colsMapping "bytecol=cf1.hbytecol, shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol"
         |)"""
        .stripMargin
    //    stmt.execute(testTableCreationHiveSQL)
    createThriftServerTable(TestHBaseTableHiveName, TestHBaseTablePreLoadName, testTableCreationHiveSQL)
    loadDataThrift(TestHBaseTableHiveName, s"$CsvPath/$DefaultLoadFile")
  }

  def loadDataThrift(tableName: String, loadFile: String) = {
    // then load data into table
    val loadSql = s"LOAD PARALL DATA LOCAL INPATH '$loadFile' INTO TABLE $tableName"
    stmt.execute(loadSql)
  }

  def createThriftServerTable(tableName: String, hbaseTable: String, creationSQL: String) = {
    val hbaseAdmin = TestHbase.hbaseAdmin
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseTable))) {
      createNativeHbaseTable(hbaseTable, TestHbaseColFamilies)
    }

    if (TestHbase.catalog.tableExists(Seq(tableName))) {
      val dropSql = s"DROP TABLE $tableName"
      stmt.execute(dropSql)
    }

    try {
      logInfo(s"invoking $creationSQL ..")
      stmt.execute(creationSQL)
    } catch {
      case e: TableExistsException =>
        logInfo("IF NOT EXISTS still not implemented so we get the following exception", e)
    }
  }

  test("StarOperator * with limit") {
    val query1 =
      s"""SELECT * FROM $TestHBaseTableHiveName LIMIT 3"""
        .stripMargin

    val result1 = stmt.executeQuery(query1)
    val exparr = Array(Array("Row1", null, 12345, 23456789, 3456789012345L, 45657.89F, 5678912.345678),
      Array("Row2", null, 12342, 23456782, 3456789012342L, 45657.82F, 5678912.345682),
      Array("Row3", null, 12343, 23456783, 3456789012343L, 45657.83F, 5678912.345683))


    var res = {
      for (rx <- 0 until 3)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"$query1 came back with $result1.length results")

    val sql2 =
      s"""SELECT * FROM $TestHBaseTableHiveName LIMIT 2"""
        .stripMargin

    val results = stmt.executeQuery(sql2)
    logInfo(s"$sql2 came back with $results.length results")
    res = {
      for (rx <- 0 until 2)
        yield compareWithTol(getData(results), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  override protected def afterAll() = {
    stmt.execute("DROP TABLE " + TestHBaseTableHiveName)
  }

  testnm = "Select all cols with filter"
  test("Select all cols with filter") {
    val query1 =
      s"""SELECT * FROM $TestHBaseTableHiveName WHERE shortcol < 12345 LIMIT 2"""
        .stripMargin

    val result1 = stmt.executeQuery(query1)
    logInfo(s"$query1 came back with $result1.length results")
    val exparr = Array(
      Array("Row2", null, 12342, 23456782, 3456789012342L, 45657.82F, 5678912.345682),
      Array("Row3", null, 12343, 23456783, 3456789012343L, 45657.83F, 5678912.345683))

    val res = {
      for (rx <- 0 until 2)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select all cols with order by"
  test("Select all cols with order by") {
    val query1 =
      s"""SELECT * FROM $TestHBaseTableHiveName WHERE shortcol < 12344 ORDER BY strcol DESC LIMIT 2"""
        .stripMargin

    val result1 = stmt.executeQuery(query1)
    val exparr = Array(
      Array("Row3", null, 12343, 23456783, 3456789012343L, 45657.83F, 5678912.345683),
      Array("Row2", null, 12342, 23456782, 3456789012342L, 45657.82F, 5678912.345682))

    val res = {
      for (rx <- 0 until 2)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select same column twice"
  test("Select same column twice") {
    val query1 =
      s"""SELECT doublecol AS double1, doublecol AS doublecol
         | FROM $TestHBaseTableHiveName
         | WHERE doublecol > 5678912.345681 AND doublecol < 5678912.345683"""
        .stripMargin

    val result1 = stmt.executeQuery(query1)
    logInfo(s"$query1 came back with $result1.length results")
    val exparr = Array(
      Array(5678912.345682, 5678912.345682))

    val res = {
      for (rx <- 0 until 1)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Select specific cols with filter"
  test("Select specific cols with filter") {
    val query1 =
      s"""SELECT doublecol AS double1, -1 * doublecol AS minusdouble,
         | substr(strcol, 2) as substrcol, doublecol, strcol,
         | bytecol, shortcol, intcol, longcol, floatcol FROM $TestHBaseTableHiveName WHERE strcol LIKE
         |  '%Row%' AND shortcol < 12345
         |  AND doublecol > 5678912.345681 AND doublecol < 5678912.345683 LIMIT 2"""
        .stripMargin

    val result1 = stmt.executeQuery(query1)
    logInfo(s"$query1 came back with $result1.length results")
    val exparr = Array(
      Array(5678912.345682, -5678912.345682, "ow2", 5678912.345682,
        "Row2", null, 12342, 23456782, 3456789012342L, 45657.82F))

    val res = {
      for (rx <- 0 until 1)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "Mixed And/or predicates"
  test("Mixed And/or predicates") {
    val query1 =
      s"""SELECT doublecol AS double1, -1 * doublecol AS minusdouble,
     substr(strcol, 2) AS substrcol, doublecol, strcol,
     bytecol, shortcol, intcol, longcol, floatcol FROM $TestHBaseTableHiveName
     WHERE strcol LIKE '%Row%'
       AND shortcol < 12345
       AND doublecol > 5678912.345681 AND doublecol < 5678912.345683
       OR (doublecol = 5678912.345683 AND strcol IS NOT NULL)
       OR (doublecol = 5678912.345683 AND strcol IS NOT NULL or intcol > 12345 AND intcol < 0)
       OR (doublecol <> 5678912.345683 AND (strcol IS NULL or intcol > 12345 AND intcol < 0))
       AND floatcol IS NOT NULL
       AND (intcol IS NOT NULL and intcol > 0)
       AND (intcol < 0 OR intcol IS NOT NULL)""".stripMargin

    val result1 = stmt.executeQuery(query1)
    logInfo(s"$query1 came back with $result1.length results")
    val exparr = Array(
      Array(5678912.345682, -5678912.345682, "ow2", 5678912.345682,
        "Row2", null, 12342, 23456782, 3456789012342L, 45657.82F),
      Array(5678912.345683, -5678912.345683, "ow3", 5678912.345683,
        "Row3", -29, 12343, 23456783, 3456789012343L, 45657.83))

    val res = {
      for (rx <- 0 until 1)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "In predicates"
  test("In predicates") {
    val query1 =
      s"""SELECT doublecol AS double1, -1 * doublecol AS minusdouble,
     substr(strcol, 2) AS substrcol, doublecol, strcol,
     bytecol, shortcol, intcol, longcol, floatcol FROM $TestHBaseTableHiveName
     WHERE doublecol IN (doublecol + 5678912.345682 - doublecol, doublecol + 5678912.345683 - doublecol)""".stripMargin

    val result1 = stmt.executeQuery(query1)
    logInfo(s"$query1 came back with $result1.length results")
    val exparr = Array(
      Array(5678912.345682, -5678912.345682, "ow2", 5678912.345682,
        "Row2", null, 12342, 23456782, 3456789012342L, 45657.82F),
      Array(5678912.345683, -5678912.345683, "ow3", 5678912.345683,
        "Row3", -29, 12343, 23456783, 3456789012343L, 45657.83))

    val res = {
      for (rx <- 0 until 1)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }

  testnm = "InSet predicates"
  test("InSet predicates") {
    val query1 =
      s"""SELECT doublecol AS double1, -1 * doublecol AS minusdouble,
     substr(strcol, 2) AS substrcol, doublecol, strcol,
     bytecol, shortcol, intcol, longcol, floatcol FROM $TestHBaseTableHiveName
     WHERE doublecol IN (5678912.345682, 5678912.345683)""".stripMargin

    val result1 = stmt.executeQuery(query1)
    logInfo(s"$query1 came back with $result1.length results")
    val exparr = Array(
      Array(5678912.345682, -5678912.345682, "ow2", 5678912.345682,
        "Row2", null, 12342, 23456782, 3456789012342L, 45657.82F),
      Array(5678912.345683, -5678912.345683, "ow3", 5678912.345683,
        "Row3", -29, 12343, 23456783, 3456789012343L, 45657.83))

    val res = {
      for (rx <- 0 until 1)
        yield compareWithTol(getData(result1), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres }
    assert(res, "One or more rows did not match expected")

    logInfo(s"Test $testnm completed successfully")
  }
}
