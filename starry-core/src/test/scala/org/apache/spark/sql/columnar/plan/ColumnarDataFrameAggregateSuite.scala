package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.{
  array_sort,
  arrays_zip,
  avg,
  col,
  collect_list,
  collect_set,
  count,
  count_distinct,
  lit,
  sum,
  sum_distinct
}
import org.apache.spark.sql.test.SQLTestData.DecimalData
import org.apache.spark.sql.{DataFrameAggregateSuite, Row}
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarDataFrameAggregateSuite
    extends DataFrameAggregateSuite
    with ColumnarSharedSparkSession
    with ParquetTest {
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
    checkAnswer(testData2.groupBy($"b").agg($"b", avg($"a")), Seq(Row(3, 4, 6, 7, 9)))
  }

  test("collect functions wit null value") {
    val df =
      Seq(("1", "2"), ("2", "2"), ("3", "4"), (null, null)).toDF("a", "b")
    checkAnswer(
      df.select(collect_list($"a"), collect_list($"b")),
      Seq(Row(Seq("1", "2", "3"), Seq("2", "2", "4"))))
    checkAnswer(
      df.select(collect_set($"a"), collect_set($"b")),
      Seq(Row(Seq("1", "2", "3"), Seq("2", "4"))))
  }
  test("test collect_set") {
    readParquetFile(testFile("model-data/case-table")) { df =>
      checkAnswer(
        df.select(
            arrays_zip(collect_set("endEvent").as("a"), collect_set("endEvent").as("b")).as("as"))
          .select(col("as.a")),
        Seq(Row(Seq("按票付款", "处理发票", "接收发票", "批准发票", "最终检查发票", "已检查并批准"))))
    }
  }
}
