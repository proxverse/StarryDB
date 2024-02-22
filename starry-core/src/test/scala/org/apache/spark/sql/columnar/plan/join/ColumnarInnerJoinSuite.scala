package org.apache.spark.sql.columnar.plan.join

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.execution.joins.InnerJoinSuite
import org.apache.spark.sql.internal.SQLConf
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarInnerJoinSuite extends InnerJoinSuite with ColumnarSharedSparkSession {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    if (!testName.startsWith("SPARK-15822")) {
      super.test(testName, testTags: _*)(testFun)
    }
//    if (testName.startsWith(
//          "inner join, null safe using SortMergeJoin (whole-stage-codegen off)")) {
//      super.test(testName, testTags: _*)(testFun)
//    }
  }
}
