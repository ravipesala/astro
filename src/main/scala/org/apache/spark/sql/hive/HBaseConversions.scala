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
import org.apache.spark.sql.hive.client.ClientInterface

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
    val columnsMapping = mr.hiveQlTable.getSerdeParam(HBaseSerDe.HBASE_COLUMNS_MAPPING)
    val storageFormat = mr.hiveQlTable.getSerdeParam(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE)
    val hbaseTableName = mr.hiveQlTable.getProperty("hbase.table.name")

    val colMap = HBaseSerDe.parseColumnsMapping(columnsMapping)
    val colMapIter = colMap.iterator()
    var keyCounter = 0
    val allColumns = new ArrayBuffer[AbstractColumn]()
    var extraParams = Map[String, String]()
    extraParams += FieldFactory.COLLECTION_SEPARATOR -> mr.tableDesc.getProperties.getProperty("colelction.delim")
    extraParams += FieldFactory.MAPKEY_SEPARATOR -> mr.tableDesc.getProperties.getProperty("mapkey.delim")
    val separators = FieldFactory.collectSeperators(extraParams)
    val encodingFormat = {
      if (storageFormat != null && storageFormat.equals("binary"))
        FieldFactory.HBASE_FORMAT
      else
        FieldFactory.STRING_FORMAT
    }
    val keyFactory = new SimpleKeyFactory(
      mr.tableDesc.getProperties.getProperty("colelction.delim").toCharArray()(0).toByte)
    //Create Astro columns from Hive table columns
    mr.hiveQlTable.getAllCols.foreach { f =>
      colMapIter.hasNext
      val col = colMapIter.next()
      if (col.isHbaseRowKey) {
        //Currently not supported the complex key types in Astro
        if (f.getType.startsWith("array<") || f.getType.startsWith("map<")) {
          throw new Exception(s"Key column type should not be complex data type.")
        } else if (f.getType.startsWith("struct<")) {
          f.getType.stripPrefix("struct<").dropRight(1).split(",").map { fs =>
            val strings = fs.split(":")
            allColumns += KeyColumn(
              strings(0),
              catalog.getDataType(strings(1)),
              keyCounter,
              FieldFactory.createFieldData(catalog.getDataType(strings(1)), encodingFormat, separators, 0, true)
            )
            keyCounter = keyCounter + 1
          }
        } else {
          allColumns += KeyColumn(
            f.getName,
            catalog.getDataType(f.getType),
            keyCounter,
            FieldFactory.createFieldData(catalog.getDataType(f.getType), encodingFormat, separators, 0, true)
          )
          keyCounter = keyCounter + 1
        }
      } else {
        allColumns += NonKeyColumn(
          f.getName,
          catalog.getDataType(f.getType),
          col.getFamilyName,
          col.getQualifierName,
          FieldFactory.createFieldData(catalog.getDataType(f.getType), encodingFormat, separators, 0, true)
        )
      }
    }
    //Create HbaseRelation with parsed columns from Hive table.
    LogicalRelation(catalog.createTable(mr.hiveQlTable.getTableName, "",
      hbaseTableName,
      allColumns, null, separators, encodingFormat, keyFactory, false))
  }

}

/**
 * This catalog extends HiveMetastoreCatalog and updates the relation if it has storage handler of type Hbase.
 * Right now this is work around as we are not yet supporting complex datatypes on key columns. Once we support complex
 * datatypes on key we can move to above class HBaseConversions.
 * @param client
 * @param hbaseContext
 */
class HBaseHiveCatalog(client: ClientInterface, hbaseContext: HBaseSQLContext)
  extends HiveMetastoreCatalog(client, hbaseContext) {

  private lazy val catalog = hbaseContext.hbaseCatalog

  override def lookupRelation(tableIdentifier: Seq[String],
                              alias: Option[String]): LogicalPlan = {
    val relation = super.lookupRelation(tableIdentifier, alias)
    relation match {
      case mr: MetastoreRelation
        if mr.tableDesc.getSerdeClassName.toLowerCase.contains("hbase") =>
        convertToHBaseRelation(mr)
      case _ => relation
    }
  }

  def convertToHBaseRelation(mr: MetastoreRelation): LogicalRelation = {
    val columnsMapping = mr.hiveQlTable.getSerdeParam(HBaseSerDe.HBASE_COLUMNS_MAPPING)
    val storageFormat = mr.hiveQlTable.getSerdeParam(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE)
    val hbaseTableName = mr.hiveQlTable.getProperty("hbase.table.name")

    val colMap = HBaseSerDe.parseColumnsMapping(columnsMapping)
    val colMapIter = colMap.iterator()
    var keyCounter = 0
    val allColumns = new ArrayBuffer[AbstractColumn]()
    var extraParams = Map[String, String]()
    extraParams += FieldFactory.COLLECTION_SEPARATOR -> mr.tableDesc.getProperties.getProperty("colelction.delim")
    extraParams += FieldFactory.MAPKEY_SEPARATOR -> mr.tableDesc.getProperties.getProperty("mapkey.delim")
    val separators = FieldFactory.collectSeperators(extraParams)
    val encodingFormat = {
      if (storageFormat != null && storageFormat.equals("binary"))
        FieldFactory.HBASE_FORMAT
      else
        FieldFactory.STRING_FORMAT
    }
    val keyFactory = new SimpleKeyFactory(
      mr.tableDesc.getProperties.getProperty("colelction.delim").toCharArray()(0).toByte)
    //Create Astro columns from Hive table columns
    mr.hiveQlTable.getAllCols.foreach { f =>
      colMapIter.hasNext
      val col = colMapIter.next()
      if (col.isHbaseRowKey) {
        //Currently not supported the complex key types in Astro
        if (f.getType.startsWith("array<") || f.getType.startsWith("map<")) {
          throw new Exception(s"Key column type should not be complex data type.")
        } else if (f.getType.startsWith("struct<")) {
          f.getType.stripPrefix("struct<").dropRight(1).split(",").map { fs =>
            val strings = fs.split(":")
            allColumns += KeyColumn(
              strings(0),
              catalog.getDataType(strings(1)),
              keyCounter,
              FieldFactory.createFieldData(catalog.getDataType(strings(1)), encodingFormat, separators, 0, true)
            )
            keyCounter = keyCounter + 1
          }
        } else {
          allColumns += KeyColumn(
            f.getName,
            catalog.getDataType(f.getType),
            keyCounter,
            FieldFactory.createFieldData(catalog.getDataType(f.getType), encodingFormat, separators, 0, true)
          )
          keyCounter = keyCounter + 1
        }
      } else {
        allColumns += NonKeyColumn(
          f.getName,
          catalog.getDataType(f.getType),
          col.getFamilyName,
          col.getQualifierName,
          FieldFactory.createFieldData(catalog.getDataType(f.getType), encodingFormat, separators, 0, true)
        )
      }
    }
    //Create HbaseRelation with parsed columns from Hive table.
    LogicalRelation(catalog.createTable(mr.hiveQlTable.getTableName, "",
      hbaseTableName,
      allColumns, null, separators, encodingFormat, keyFactory, false))
  }

}
