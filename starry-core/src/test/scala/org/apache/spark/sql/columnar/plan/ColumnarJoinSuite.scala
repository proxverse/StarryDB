package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.{DataFrameAggregateSuite, JoinSuite, Row}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.functions.{avg, count, countDistinct, sum_distinct}
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarJoinSuite extends JoinSuite with ColumnarSharedSparkSession {
  import testImplicits._
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    val ignores = Seq(
      "join operator selection", //smg
      "broadcasted existence join operator selection",
      "cross join with broadcast",
      "test SortMergeJoin (with spill)",
      "test SortMergeJoin output ordering",
      "SPARK-22445 Respect stream-side child's needCopyResult in BroadcastHashJoin",
      "SPARK-26352: join reordering should not change the order of columns", //cross join
      "NaN and -0.0 in join keys", // normalize_nanand_zero
      "SPARK-35984: Config to force applying shuffled hash join",
      "SPARK-34593: Preserve broadcast nested loop join partitioning and ordering",
      "SPARK-32399: Full outer shuffled hash join",
      "SPARK-32383: Preserve hash join (BHJ and SHJ) stream side ordering",
      "SPARK-28323: PythonUDF should be able to use in join condition",
      "SPARK-28345: PythonUDF predicate should be able to pushdown to join",
      "SPARK-32649: Optimize BHJ/SHJ inner/semi join with empty hashed relation",
      "SPARK-32330: Preserve shuffled hash join build side partitioning")
    if (!ignores.contains(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
  }
  test("tests") {
    spark.sharedState.cacheManager.clearCache()
    sql("CACHE TABLE testData")
    sql("CACHE TABLE testData2")
    Seq(
      ("SELECT * FROM testData LEFT JOIN testData2 ON key = a", classOf[BroadcastHashJoinExec]),
//      ("SELECT * FROM testData RIGHT JOIN testData2 ON key = a where key = 2",
//          classOf[BroadcastHashJoinExec]),
//      ("SELECT * FROM testData right join testData2 ON key = a and key = 2",
//          classOf[BroadcastHashJoinExec])
    ).foreach(assertJoin)
  }
}
