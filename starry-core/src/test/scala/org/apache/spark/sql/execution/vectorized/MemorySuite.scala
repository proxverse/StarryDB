/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.vectorized

import com.prx.starry.Starry
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.columnar.cache.CachedVeloxBatch
import org.apache.spark.sql.execution.columnar.extension.utils.NativeLibUtil
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.StarryConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.Assertions

class MemorySuite extends ParquetTest {

  test("test root memory pool") {
    if (System.getenv("RUN_MEMORY_POOL_TEST") != null) {
      withTable("bucket_table") {
        val conf1 = spark.sparkContext.conf
        conf1.set("spark.sql.starry.maxRootMemoryBytes", "10M")
        conf1.set("spark.sql.starry.maxQueryMemoryBytes", "1KB")
        NativeLibUtil.init(StarryConf.getAllConfJson(conf1, "spark.sql.starry"))
        try {
          val frame = readResourceParquetFile("performance-data")
          val frame1 = frame
          frame1.count()
          Assertions.fail()
        } catch {
          case exception: Exception =>
            if (!exception.getMessage.contains("Exceeded memory pool cap of 1.00KB")) {
              Assertions.fail()
            }
        }
        try {
          val frame = readResourceParquetFile("performance-data")
          val frame1 = frame
          frame1.cache
          frame1.count()
          Assertions.fail()
        } catch {
          case exception: Exception =>
            if (!exception.getMessage.contains("Exceeded memory pool cap of 10.00MB")) {
              Assertions.fail()
            }
        }

      }
    }
  }

  override protected def spark: SparkSession = {
    val conf = new SparkConf()
    Starry.starrySession(conf)
  }
}
