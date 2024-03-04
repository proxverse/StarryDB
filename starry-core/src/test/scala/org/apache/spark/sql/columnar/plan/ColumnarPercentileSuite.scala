package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.ApproximatePercentileQuerySuite
import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.scalactic.source.Position
import org.scalatest.Tag

class ColumnarPercentileSuite
    extends ApproximatePercentileQuerySuite
    with ColumnarSharedSparkSession {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    val ignoreTests: Set[String] = Set() // need row type
    if (!ignoreTests.contains(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
  }


  test("percentile with countD") {
    withTempView("pv") {
      testData2.createOrReplaceTempView("pv")
      val ret1 = spark.sql("select " +
        "percentile_approx(a, 0.3, 1000), " +
        "count(distinct a) " +
        "from pv group by b")
      ret1.show()
    }
  }
}
