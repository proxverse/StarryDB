package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetIOSuite
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.scalactic.source.Position
import org.scalatest.Tag

class NativeParquetIOSuite extends ParquetIOSuite with ColumnarSharedSparkSession {
  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    if (!(testName.startsWith("SPARK-7837") || testName.startsWith("SPARK-11044")
          || testName.startsWith("SPARK-6330")
          || testName.startsWith("SPARK-35640") // for async
          || testName.startsWith("Standard mode - fixed-length decimals") // cast
          || testName.startsWith("Legacy mode - fixed-length decimals") // cast
          || testName.startsWith("Legacy mode - fixed-length decimals") // cast
        )) {
      super.test(testName, testTags: _*)(testFun)
    }
  }

  test("sa") {
    val strings = new Array[String](50)
    for (i <- 0 until strings.length) {
      strings(i) = "YourTestString" // 假设这是你的字符串

    }

    val str = strings.mkString(",")
    val builder = new NativePlanBuilder()
    val startTime = System.nanoTime
    Range(0, 10000).foreach(i => builder.nativeTestString(str))
    val endTime = System.nanoTime

    System.out.println("JNI call took " + (endTime - startTime) / 1000 / 1000 + " nanoseconds.")
  }

  test("1 vectorized reader: array") {
    val data = Seq(Tuple1(null), Tuple1(Seq()), Tuple1(Seq("a", "b", "c")), Tuple1(Seq(null)))

    withParquetFile(data) { file =>
      readParquetFile(file) { df =>
        checkAnswer(
          df.sort("_1"),
          Row(null) :: Row(Seq()) :: Row(Seq(null)) :: Row(Seq("a", "b", "c")) :: Nil)
      }
    }
  }

  test("1null and non-null strings") {
    // Create a dataset where the first values are NULL and then some non-null values. The
    // number of non-nulls needs to be bigger than the ParquetReader batch size.
    val data: Dataset[String] = spark
      .range(200)
      .map(i =>
        if (i < 150) null
        else "a")
    val df = data.toDF("col")
    assert(df.agg("col" -> "count").collect().head.getLong(0) == 50)

    withTempPath { dir =>
      val path = s"${dir.getCanonicalPath}/data"
      df.write.parquet(path)

      readParquetFile(path) { df2 =>
        assert(df2.agg("col" -> "count").collect().head.getLong(0) == 50)
      }
    }
  }
}
