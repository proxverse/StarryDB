package org.apache.spark.sql.shuffle

import com.prx.starry.Starry
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.StarryConf

class StreamingShuffleManagerSuite extends ParquetTest {

  test("memory statics") {

  }

  test("remove executor") {

  }

  test("shuffle benchmark") {
    withTable("bucket_table") {
      Range(0, 3).foreach { i =>
        val frame = readResourceParquetFile("performance-data")
        frame
          .groupBy("ORDERKEY")
          .agg(expr("max(DISCOUNT)"))
          .collect()
        frame.schema
      }
      Thread.sleep(5000000)
    }
  }

  override protected def spark: SparkSession = {
    val conf1 = new SparkConf()
    conf1.set("spark.sql.starry.columnar.columnarShuffleEnabled", "true")
    conf1.set(StarryConf.STREAMING_SHUFFLE_ENABLED.key, "true")
    conf1.set("spark.sql.shuffle.partitions", "2")
    conf1.setMaster("local[4]")
    Starry.starrySession(conf1)
  }
}
