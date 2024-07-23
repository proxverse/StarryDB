package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.columnar.expressions.BitmapContains
import org.apache.spark.sql.execution.columnar.expressions.aggregate.{BitmapConstructAggFunction, BitmapCountDistinctAggFunction}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.{array_sort, arrays_zip, avg, col, collect_list, collect_set, count, count_distinct, lit, map, max_by, min_by, sum, sum_distinct}
import org.apache.spark.sql.test.SQLTestData.{DecimalData, TestData2}
import org.apache.spark.sql.{AnalysisException, Column, DataFrameAggregateSuite, Row}
import org.scalactic.source.Position
import org.scalatest.Tag

case class LongTestData(a: Long, b: Int)

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
      "multiple column distinct count",
      "moments",
      "zero moments",
      "SPARK-14664: Decimal sum/avg over window should work.",
      "SPARK-19471: AggregationIterator does not initialize the generated result projection before using it",
      "SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle",
      "SPARK-26021: NaN and -0.0 in grouping expressions",
      "SPARK-31620: agg with subquery (whole-stage-codegen = true)",
      "SPARK-31620: agg with subquery (whole-stage-codegen = false)",
      "SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate",
      "SPARK-35412: groupBy of year-month/day-time intervals should work",
      "SPARK-36926: decimal average mistakenly overflow",
      "SPARK-36926: decimal average mistakenly overflow",
      "max_by",
      "min_by") // need row type
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

  test("collect functions wit null value") {
    val df =
      Seq(("1", "2"), ("2", "2"), ("3", "4"), (null, null)).toDF("a", "b")
    checkAnswer(
      df.select(collect_list($"a"), collect_list($"b")),
      Seq(Row(Seq("1", "2", "3"), Seq("2", "2", "4"))))
    checkAnswer(
      df.select(collect_set($"a"), collect_set($"b")),
      Seq(Row(Seq("1", "2", "3"), Seq("2", "4"))))

    val df2 = Seq((null.asInstanceOf[String], null.asInstanceOf[String])).toDF("a", "b")
    checkAnswer(df2.select(collect_list($"a"), collect_list($"b")), Seq(Row(Seq(), Seq())))
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

  test("max by") {
    val yearOfMaxEarnings =
      sql("SELECT course, max_by(year, earnings) FROM courseSales GROUP BY course")
    checkAnswer(yearOfMaxEarnings, Row("dotNET", 2013) :: Row("Java", 2013) :: Nil)

    checkAnswer(
      courseSales.groupBy("course").agg(max_by(col("year"), col("earnings"))),
      Row("dotNET", 2013) :: Row("Java", 2013) :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y)"),
      Row("b") :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', 10)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', null)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', null)) AS tab(x, y)"),
      Row("b") :: Nil
    )
//
//    checkAnswer(
//      sql("SELECT max_by(x, y) FROM VALUES (('a', null)), (('b', null)) AS tab(x, y)"),
//      Row(null) :: Nil
//    )

    // structs as ordering value.
    checkAnswer(
      sql("select max_by(x, y) FROM VALUES (('a', (10, 20))), (('b', (10, 50))), " +
        "(('c', (10, 60))) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("select max_by(x, y) FROM VALUES (('a', (10, 20))), (('b', (10, 50))), " +
        "(('c', null)) AS tab(x, y)"),
      Row("b") :: Nil
    )

    withTempView("tempView") {
      val dfWithMap = Seq((0, "a"), (1, "b"), (2, "c"))
        .toDF("x", "y")
        .select($"x", map($"x", $"y").as("y"))
        .createOrReplaceTempView("tempView")
      val error = intercept[AnalysisException] {
        sql("SELECT max_by(x, y) FROM tempView").show
      }
      assert(
        error.message.contains("function max_by does not support ordering on type map<int,string>"))
    }
  }

  test("min by") {
    val yearOfMinEarnings =
      sql("SELECT course, min_by(year, earnings) FROM courseSales GROUP BY course")
    checkAnswer(yearOfMinEarnings, Row("dotNET", 2012) :: Row("Java", 2012) :: Nil)

    checkAnswer(
      courseSales.groupBy("course").agg(min_by(col("year"), col("earnings"))),
      Row("dotNET", 2012) :: Row("Java", 2012) :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y)"),
      Row("a") :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', 10)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("a") :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', null)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', null)) AS tab(x, y)"),
      Row("a") :: Nil
    )

//    checkAnswer(
//      sql("SELECT min_by(x, y) FROM VALUES (('a', null)), (('b', null)) AS tab(x, y)"),
//      Row(null) :: Nil
//    )

    // structs as ordering value.
    checkAnswer(
      sql("select min_by(x, y) FROM VALUES (('a', (10, 20))), (('b', (10, 50))), " +
        "(('c', (10, 60))) AS tab(x, y)"),
      Row("a") :: Nil
    )

    checkAnswer(
      sql("select min_by(x, y) FROM VALUES (('a', null)), (('b', (10, 50))), " +
        "(('c', (10, 60))) AS tab(x, y)"),
      Row("b") :: Nil
    )

    withTempView("tempView") {
      val dfWithMap = Seq((0, "a"), (1, "b"), (2, "c"))
        .toDF("x", "y")
        .select($"x", map($"x", $"y").as("y"))
        .createOrReplaceTempView("tempView")
      val error = intercept[AnalysisException] {
        sql("SELECT min_by(x, y) FROM tempView").show
      }
      assert(
        error.message.contains("function min_by does not support ordering on type map<int,string>"))
    }
  }

  test("bitmap count distinct") {
    val bitmap_count_distinct = (child: Column) =>
      Column(BitmapCountDistinctAggFunction(child.expr).toAggregateExpression(false))

    checkAnswer(
      testData3.agg(
        count($"a"),
        count($"b"),
        count(lit(1)),
        bitmap_count_distinct($"a"),
        bitmap_count_distinct($"b")),
      Row(2, 1, 2, 2, 1))

    checkAnswer(testData3.agg("a" -> "bitmap_count_distinct"), Row(2))

    val longDf = spark.sparkContext
      .parallelize(
        LongTestData(1L, 1) ::
          LongTestData(1L, 1) ::
          LongTestData(2L, 1) ::
          LongTestData(2L, 2) ::
          LongTestData(3L, 2) :: Nil,
        2)
      .toDF()
    checkAnswer(
      longDf.groupBy($"b").agg(bitmap_count_distinct($"a")),
      Row(1, 2) :: Row(2, 2) :: Nil)
  }

  test("bitmap contains") {
    val bitmap = testData3.filter($"a".equalTo(1)).agg(
      Column(BitmapConstructAggFunction($"a".expr).toAggregateExpression(false))
    ).collect().head.get(0).asInstanceOf[Array[Byte]]

    assert(testData3.filter(Column(BitmapContains($"a".expr, lit(bitmap).expr))).count() == 1)
  }

}
