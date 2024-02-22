package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.columnar.expressions.Unnest
import org.apache.spark.sql.{AnalysisException, Column, GeneratorFunctionSuite, Row}
import org.apache.spark.sql.functions._
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarGeneratorFunctionSuite
    extends GeneratorFunctionSuite
    with ColumnarSharedSparkSession {
  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
//    if ((testName.contains("generator 2 in aggregate expression"))) {
      super.test(testName, testTags: _*)(testFun)
//    }
//    if (!(testName.contains("inline on column") || testName.contains("TimestampNTZType"))) {
//      super.test(testName, testTags: _*)(testFun)
//    }
  }



  test("2self join explode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    val exploded = df.select(explode($"intList").as("i"))

    val frame1 = exploded.agg(count("i")).collect()
//
    val frame = exploded.as("a")
    checkAnswer(
      frame.join(exploded, frame("a.i") === exploded("i")).agg(count("a.i")),
      Row(3) :: Nil)
  }

  test("single explode_outer2") {
    val df = Seq((1, Seq(1, 2, 3)), (2, Seq())).toDF("a", "intList")
    checkAnswer(
      df.select(explode_outer($"intList")),
      Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)
  }

  test("unnest on column2") {

    // stack on column data
    val df2 = Seq((2, 1, 2, 3)).toDF("n", "a", "b", "c")
    checkAnswer(df2.selectExpr("stack(2, a, b, c)"), Row(1, 2) :: Row(3, null) :: Nil)


    val df = Seq((1, 2)).toDF("a", "b")

    checkAnswer(
      df.select(new Column(new Unnest(Seq(expr("array(a, a)").expr)))),
      Row(1) :: Row(1) :: Nil)
  }

  test("1SPARK-30997: generators in aggregate expressions for dataframe") {
    val df = Seq(1, 2, 3).toDF("v")
    checkAnswer(df.select(explode(array(min($"v"), max($"v")))), Row(1) :: Row(3) :: Nil)
  }

  test("inline on column2") {

    val df = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("inline(array(struct(a), named_struct('a', 2)))"),
      Row(1) :: Row(2) :: Nil)
    checkAnswer(
      df.selectExpr("inline(array(struct(a), struct(a)))"),
      Row(1) :: Row(1) :: Nil)

    checkAnswer(
      df.selectExpr("inline(array(struct(a, b), struct(a, b)))"),
      Row(1, 2) :: Row(1, 2) :: Nil)

    // Spark think [struct<a:int>, struct<b:int>] is heterogeneous due to name difference.
    val m = intercept[AnalysisException] {
      df.selectExpr("inline(array(struct(a), struct(b)))")
    }.getMessage
    assert(m.contains("data type mismatch"))

    checkAnswer(
      df.selectExpr("inline(array(struct(a), named_struct('a', b)))"),
      Row(1) :: Row(2) :: Nil)

    // Spark think [struct<a:int>, struct<col1:int>] is heterogeneous due to name difference.
    val m2 = intercept[AnalysisException] {
      df.selectExpr("inline(array(struct(a), struct(2)))")
    }.getMessage
    assert(m2.contains("data type mismatch"))

    checkAnswer(
      df.selectExpr("inline(array(struct(a), named_struct('a', 2)))"),
      Row(1) :: Row(2) :: Nil)

    checkAnswer(
      df.selectExpr("struct(a)").selectExpr("inline(array(*))"),
      Row(1) :: Nil)

    checkAnswer(
      df.selectExpr("array(struct(a), named_struct('a', b))").selectExpr("inline(*)"),
      Row(1) :: Row(2) :: Nil)
  }
}
