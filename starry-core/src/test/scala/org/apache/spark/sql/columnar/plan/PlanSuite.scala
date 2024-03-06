package org.apache.spark.sql.columnar.plan

import org.apache.spark.sql.common.ColumnarSharedSparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest

class PlanSuite extends ColumnarSharedSparkSession with ParquetTest {

  test("test plan builder and dictionary vector") {
    readParquetFile(testFile("test-data/memory_leak_test.parquet")) { df =>
      df.filter("caseid = 'ABC-20221130-33888'")
        .collect()
    }
  }

  test("test row cache") {
    readParquetFile(testFile("test-data/memory_leak_test.parquet")) { df =>
      val rows = df.filter("caseid = 'ABC-20221130-33888'").cache.count
      println(rows)
    }
  }
  test("test columnar cache") {
    readParquetFile(testFile("test-data/memory_leak_test.parquet")) { df =>
      val rows = df
        .filter("caseid = 'ABC-20221130-33888'")
        .cache
        .collect()
      println(rows)
    }
  }
}
