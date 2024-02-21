package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.{
  GlobalLimitExec,
  ReferenceSort,
  SortExec,
  SortSuite,
  SparkPlan
}
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarSortSuite extends SortSuite with ColumnarSharedSparkSession {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    if (!(testName.contains("Interval") || testName.contains("TimestampNTZType"))) {
      super.test(testName, testTags: _*)(testFun)
    }
  }
}
