package org.apache.spark.sql.shuffle

import com.prx.starry.Starry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.expr

class ShuffleManagerSuite extends ParquetTest {

  test("memory statics") {

  }

  test("remove executor") {

  }

  ignore("shuffle benchmark") {
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

  override protected def spark: SparkSession = Starry.starrySession()
}
