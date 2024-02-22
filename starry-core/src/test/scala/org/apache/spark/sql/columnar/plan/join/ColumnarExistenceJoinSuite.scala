package org.apache.spark.sql.columnar.plan.join

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.joins.{ExistenceJoinSuite, InnerJoinSuite}
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarExistenceJoinSuite extends ExistenceJoinSuite with ColumnarSharedSparkSession {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    if (!testName.startsWith("SPARK-15822")) {
      super.test(testName, testTags: _*)(testFun)
    }
//    if (testName.startsWith("inner join, null safe using BroadcastHashJoin (build=left) (whole-stage-codegen off)")) {
//      super.test(testName, testTags: _*)(testFun)
//    }
  }
}
