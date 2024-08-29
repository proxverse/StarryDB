package org.apache.spark.sql.shuffle

import com.prx.starry.Starry
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.{col, expr}
import org.scalatest.Assertions

class ShuffleManagerSuite extends ParquetTest {
  test("writer table ") {
    withTable("bucket_table") {
      val frame = readResourceParquetFile("performance-data")
      frame
        .groupBy("ORDERKEY")
        .agg(expr("max(DISCOUNT)").as("ma"))
        .repartition(2, col("ma"))
        .write
        .saveAsTable("bucket_table")
    }
  }
  test("normal shuffle") {
    withTable("bucket_table") {
      Range(0, 3).foreach { i =>
        val frame = readResourceParquetFile("performance-data")
        val rows = frame
          .groupBy("ORDERKEY")
          .agg(expr("max(DISCOUNT)"))
          .collect()
        Assertions.assert(rows.length == 1500000)
      }
    }
  }

  override protected def spark: SparkSession = {
    val conf1 = new SparkConf()
    conf1.set("spark.sql.starry.columnar.columnarShuffleEnabled", "true")
    conf1.set("spark.sql.shuffle.partitions", "2")
    conf1.setMaster("local[4]")
    Starry.starrySession(conf1)
  }
}
