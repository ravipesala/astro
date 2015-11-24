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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.hbase.HBaseSerDe
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hbase._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * When scanning Metastore Hbase tables, convert them to Hbase
 * data source relations for better performance.
 *
 */
case class HBaseConversions(sqlContext: SQLContext) extends Rule[LogicalPlan] {

  private lazy val catalog = sqlContext.asInstanceOf[HBaseSQLContext].hbaseCatalog

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved) {
      return plan
    }

    // Collects all `MetastoreRelation`s which should be replaced
    val toBeReplaced = plan.collect {
      // Read path
      case p@PhysicalOperation(_, _, relation: MetastoreRelation)
        if relation.tableDesc.getSerdeClassName.toLowerCase.contains("hbase") =>
        val hbaseRelation = convertToHBaseRelation(relation)
        val attributedRewrites = relation.output.zip(hbaseRelation.output)
        (relation, hbaseRelation, attributedRewrites)
    }

    val relationMap = toBeReplaced.map(r => (r._1, r._2)).toMap
    val attributedRewrites = AttributeMap(toBeReplaced.map(_._3).fold(Nil)(_ ++: _))

    // Replaces all `MetastoreRelation`s with corresponding `HbaseRelation`s, and fixes
    // attribute IDs referenced in other nodes.
    plan.transformUp {
      case r: MetastoreRelation if relationMap.contains(r) =>
        val hbaseRelation = relationMap(r)
        val alias = r.alias.getOrElse(r.tableName)
        Subquery(alias, hbaseRelation)

      case other => other.transformExpressions {
        case a: Attribute if a.resolved => attributedRewrites.getOrElse(a, a)
      }
    }
  }

  def convertToHBaseRelation(mr: MetastoreRelation): LogicalRelation = {
    val columnsMapping = mr.hiveQlTable.getSerdeParam("hbase.columns.mapping")
    val hbaseTableName = mr.hiveQlTable.getProperty("hbase.table.name")

    val colMap = HBaseSerDe.parseColumnsMapping(columnsMapping)
    val colMapIter = colMap.iterator()
    var keyCounter = 0
    val allColumns = new ArrayBuffer[AbstractColumn]()
    //Create Astro columns from Hive table columns
    mr.hiveQlTable.getAllCols.foreach { f =>
      colMapIter.hasNext
      val col = colMapIter.next()
      if (col.isHbaseRowKey) {
        //If key is struct type, split them into individual columns in Astro.
        if (f.getType.startsWith("struct<")) {
          val fields = f.getType.stripPrefix("struct<").dropRight(1).split(",").map(_.trim)
          fields.foreach { fd =>
            val field = fd.split(":").map(_.trim)
            allColumns += KeyColumn(
              field(0),
              catalog.getDataType(field(1)),
              keyCounter
            )
            keyCounter = keyCounter + 1
          }
        } else {
          allColumns += KeyColumn(
            f.getName,
            catalog.getDataType(f.getType),
            keyCounter
          )
          keyCounter = keyCounter + 1
        }
      } else {
        allColumns += NonKeyColumn(
          f.getName,
          catalog.getDataType(f.getType),
          col.getFamilyName,
          col.getQualifierName
        )
      }
    }
    //Create HbaseRelation with parsed columns from Hive table.
    LogicalRelation(catalog.createTable(mr.hiveQlTable.getTableName, "", hbaseTableName, allColumns, null, "hbasebinaryformat"))
  }

}
