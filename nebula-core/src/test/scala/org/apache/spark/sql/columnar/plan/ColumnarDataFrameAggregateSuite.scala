package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.functions.{count, count_distinct, sum, sum_distinct}
import org.apache.spark.sql.{DataFrameAggregateSuite, Row}
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarDataFrameAggregateSuite
    extends DataFrameAggregateSuite
    with ColumnarSharedSparkSession {
  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    val ignoreTests: Set[String] = Set(
      "SPARK-17237 remove backticks in a pivot result schema",
      "grouping/grouping_id inside window function",
      "collect functions structs",
      "SPARK-31500: collect_set() of BinaryType returns duplicate elements",
      "collect functions should not collect null values",
      "multiple column distinct count") // need row type
    if (!ignoreTests.contains(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
    //    }
//    if (testName.startsWith("inner join, null safe using BroadcastHashJoin
    //    (build=left) (whole-stage-codegen off)")) {
//      super.test(testName, testTags: _*)(testFun)
//    }
  }

  test("count2") {
    assert(testData2.count() === testData2.rdd.map(_ => 1).count())

    checkAnswer(
      testData2.agg(count($"a"), sum_distinct($"a")), // non-partial
      Row(6, 6.0))
  }

  test("mulitily count distinct") {
    checkAnswer(
      testData2.agg(sum($"a"), count_distinct($"a"), count_distinct($"b")),
      Row(2, 1, 2, 2, 1)
    )
  }
}
