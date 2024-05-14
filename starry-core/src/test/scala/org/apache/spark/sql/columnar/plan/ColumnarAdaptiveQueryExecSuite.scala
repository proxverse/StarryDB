package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveQueryExecSuite
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarAdaptiveQueryExecSuite
    extends AdaptiveQueryExecSuite
    with ColumnarSharedSparkSession {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
    implicit pos: Position): Unit = {
  }
}
