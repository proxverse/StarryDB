package org.apache.spark.sql.columnar.plan

import com.prx.starry.Starry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._

class ColumnarUTSuite extends ParquetTest {
  override protected def spark: SparkSession = Starry.starrySession()

  test("test expression caseSensitive") {
    val rows = readResourceParquetFile("performance-data")
      .filter(expr("PARTKEY > 0"))
      .select(col("commEnt"))
      .collect()
    rows
  }
}
