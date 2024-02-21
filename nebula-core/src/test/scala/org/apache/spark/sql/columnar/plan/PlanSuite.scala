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
    val c1 = 'a.boolean
    val c4 = 'b.string
    val c6 = 'c.string
    val when = CaseWhen(Seq((c1, c4)), c6)
    val structType = StructType.fromAttributes(Seq(c1, c4, c6))
    val nativeExpression = ExpressionConvert.convertToNative(when).asInstanceOf[NativeExpression]
    val l = NativeExpressionConvert.nativeDeserializeExpr(
      "{\"functionName\":\"eq\",\"inputs\":[{\"fieldName\":\"dict\",\"type\":{\"type\":\"VARCHAR\",\"name\":\"Type\"},\"name\":\"FieldAccessTypedExpr\"},{\"valueVector\":\"AQAAACAAAAB7InR5cGUiOiJWQVJDSEFSIiwibmFtZSI6IlR5cGUifQEAAAAAARIAAABBQkMtwLDjIAEAAAASAAAAQUJDLTIwMjIwMTA0LTAwMDE2\",\"type\":{\"type\":\"VARCHAR\",\"name\":\"Type\"},\"name\":\"ConstantTypedExpr\"}],\"type\":{\"type\":\"BOOLEAN\",\"name\":\"Type\"},\"name\":\"CallTypedExpr\"}")
    val builder = new NativePlanBuilder()
    val builder1 = builder
      .scan(structType)
    val str1 = builder.nodeId()
    val str = builder1
      .project(Array("p1"), Array(nativeExpression.handle))
      .builderAndRelease()

    str
  }

  test("test") {
    val rows = spark.read
      .parquet(
        "/Users/xuyiming/data2/warehouse/pool_1.db/t_3_case_table_21/part-00000-1b0274e3-75b1-4bb4-ae89-ea71cdc61197_00000.c000.snappy.parquet")
//      .filter("preCaseId = 'ABC-20220103-00005-312'")
      .select(col("preCaseId"), element_at(col("sortedEventList"), 1))
      .limit(10)
      .collect()
    rows
    Thread.sleep(100)
  }

  test("test join") {
    val rows = spark.read
        .parquet(
          "/Users/xuyiming/data2/warehouse/pool_1.db/t_3_case_table_21/part-00000-1b0274e3-75b1-4bb4-ae89-ea71cdc61197_00000.c000.snappy.parquet")
        //      .filter("preCaseId = 'ABC-20220103-00005-312'")
        .select(col("preCaseId").as("A1"), element_at(col("sortedEventList"), 1).as("B1"))


    val value = spark.read
        .parquet(
          "/Users/xuyiming/data2/warehouse/pool_1.db/t_3_case_table_21/part-00000-1b0274e3-75b1-4bb4-ae89-ea71cdc61197_00000.c000.snappy.parquet")
        //      .filter("preCaseId = 'ABC-20220103-00005-312'")
        .select(col("preCaseId").as("A2"), element_at(col("sortedEventList"), 1).as("B2"))
        .join(rows, expr("A1 = A2"), "inner")
        .limit(10)
    val plan = value.queryExecution.executedPlan
    val rows1 = value
        .collect()


    rows
    Thread.sleep(100)
  }

}
