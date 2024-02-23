package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, NativeExpression}
import org.apache.spark.sql.execution.columnar.jni.{NativeExpressionConvert, NativePlanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

class PlanSuite extends ColumnarSharedSparkSession {

  test("test plan builder ") {

    val rows = spark.read.parquet("/Users/xuyiming/data4/warehouse/pool_1.db/t_orderevent_3/part-00000-e9da698f-6d79-4b8f-b2a5-0635101aaed4-c000.snappy.parquet")
      .filter("caseid = 'ABC-20221130-33888'")
      .select("caseid")
      .collect()

    println(rows)
  }
}
