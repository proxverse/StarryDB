package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.{DataFrame, DataFrameJoinSuite}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.columnar.extension.plan.ColumnarBroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarDataFrameJoinSuite extends DataFrameJoinSuite with ColumnarSharedSparkSession {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    val ignores = Seq("Supports multi-part names for broadcast hint resolution")
    if (!ignores.contains(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
  }

  test("column -> Supports multi-part names for broadcast hint resolution") {
    val (table1Name, table2Name) = ("t1", "t2")

    withTempDatabase { dbName =>
      withTable(table1Name, table2Name) {
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          spark.range(50).write.saveAsTable(s"$dbName.$table1Name")
          spark.range(100).write.saveAsTable(s"$dbName.$table2Name")

          def checkIfHintApplied(df: DataFrame): Unit = {
            val sparkPlan = df.queryExecution.executedPlan
            val broadcastHashJoins = collect(sparkPlan) { case p: BroadcastHashJoinExec => p }
            assert(broadcastHashJoins.size == 1)
            val broadcastExchanges = broadcastHashJoins.head.collect {
              case p: ColumnarBroadcastExchangeExec => p
            }
            assert(broadcastExchanges.size == 1)
            val tables = broadcastExchanges.head.collect {
              case FileSourceScanExec(_, _, _, _, _, _, _, Some(tableIdent), _) => tableIdent
            }
            assert(tables.size == 1)
            assert(tables.head === TableIdentifier(table1Name, Some(dbName)))
          }

          def checkIfHintNotApplied(df: DataFrame): Unit = {
            val sparkPlan = df.queryExecution.executedPlan
            val broadcastHashJoins = collect(sparkPlan) { case p: BroadcastHashJoinExec => p }
            assert(broadcastHashJoins.isEmpty)
          }

          def sqlTemplate(tableName: String, hintTableName: String): DataFrame = {
            sql(
              s"SELECT /*+ BROADCASTJOIN($hintTableName) */ * " +
                s"FROM $tableName, $dbName.$table2Name " +
                s"WHERE $tableName.id = $table2Name.id")
          }

          def dfTemplate(tableName: String, hintTableName: String): DataFrame = {
            spark
              .table(tableName)
              .join(spark.table(s"$dbName.$table2Name"), "id")
              .hint("broadcast", hintTableName)
          }

          sql(s"USE $dbName")

          checkIfHintApplied(sqlTemplate(table1Name, table1Name))
          checkIfHintApplied(sqlTemplate(s"$dbName.$table1Name", s"$dbName.$table1Name"))
          checkIfHintApplied(sqlTemplate(s"$dbName.$table1Name", table1Name))
          checkIfHintNotApplied(sqlTemplate(table1Name, s"$dbName.$table1Name"))

          checkIfHintApplied(dfTemplate(table1Name, table1Name))
          checkIfHintApplied(dfTemplate(s"$dbName.$table1Name", s"$dbName.$table1Name"))
          checkIfHintApplied(dfTemplate(s"$dbName.$table1Name", table1Name))
          checkIfHintApplied(dfTemplate(table1Name, s"$dbName.$table1Name"))
          checkIfHintApplied(
            dfTemplate(table1Name, s"${CatalogManager.SESSION_CATALOG_NAME}.$dbName.$table1Name"))

          withView("tv") {
            sql(s"CREATE VIEW tv AS SELECT * FROM $dbName.$table1Name")
            checkIfHintApplied(sqlTemplate("tv", "tv"))
            checkIfHintNotApplied(sqlTemplate("tv", s"$dbName.tv"))

            checkIfHintApplied(dfTemplate("tv", "tv"))
            checkIfHintApplied(dfTemplate("tv", s"$dbName.tv"))
          }
        }
      }
    }
  }
}
