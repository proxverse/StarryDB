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

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.common.{ColumnarSharedSparkSession, ValidateFunSuite}
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.columnar.cache.CachedVeloxBatch
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class CacheSuite
  extends QueryTest
  with ParquetTest
  with ValidateFunSuite
  with ColumnarSharedSparkSession {

  test("PostMergeAggregateExpression: test rewrite") {

    withTable("bucket_table") {
      val frame = readResourceParquetFile("performance-data")
      val frame1 = frame
      frame1.cache
      frame1.count()
      val cachedFrame = frame
        .select(split(col("SHIPMODE"), " ")
          .as("a"))
        .cache()
      val rows1 = cachedFrame
        .select(element_at(col("a"), 0))
        .count()
      val rows = cachedFrame.collect() // test column to row
      spark.sharedState.cacheManager.clearCache()
//        val batches = VeloxColumnarBatch.map.asScala.filterNot(_.isClosed())
//        assert(batches.isEmpty)
//        val batches1 = WritableVeloxColumnVector.map.asScala.filterNot(_.isClosed())
//        assert(batches1.isEmpty)
//        val batches2 = ReadableVeloxColumnVector.map.asScala.filterNot(_.isClosed())
//        assert(batches2.isEmpty)

    }
  }

  test("PostMergeAggregateExpression: test seri") {
    withTable("bucket_table") {
      val frame = readResourceParquetFile("performance-data")
      val frame1 = frame
      frame1.cache
      frame1.count()
      val variants = frame
        .select(col("SHIPMODE")
          .as("a")).collect()

      val schema = StructType(Seq(StructField("t", StringType)))
      val batch = ColumnBatchUtils.createWriterableColumnBatch(variants.length, schema)
      val start = System.currentTimeMillis()
      Range(0, variants.length)
        .foreach {
          i =>
            batch
              .column(0)
              .asInstanceOf[WritableColumnVector]
              .putByteArray(i, variants.apply(i).getString(0).getBytes())
        }
      batch.setNumRows(variants.length)

      val batch1 = new CachedVeloxBatch(batch.asInstanceOf[VeloxColumnarBatch], schema)
      val serializer = new JavaSerializer(new SparkConf())
      val instance = serializer.newInstance()
      val batch2 = instance.deserialize[CachedVeloxBatch](instance.serialize(batch1))

      Range(0, variants.length)
        .foreach {
          i =>
            variants.apply(i).getString(0) == batch2.veloxBatch.getRow(i).getString(0)
        }
      println(batch2)
    }
  }


  test("PostMergeAggregateExpression: kryo seri") {
    withTable("bucket_table") {
      val frame = readResourceParquetFile("performance-data")
      val frame1 = frame
      frame1.cache
      frame1.count()
      val variants = frame
        .select(col("SHIPMODE")
          .as("a"))
        .limit(100000)
        .collect()

      val schema = StructType(Seq(StructField("t", StringType)))
      val batch = ColumnBatchUtils.createWriterableColumnBatch(variants.length, schema)
      Range(0, variants.length)
        .foreach {
          i =>
            batch
              .column(0)
              .asInstanceOf[WritableColumnVector]
              .putByteArray(i, variants.apply(i).getString(0).getBytes())
        }
      batch.setNumRows(variants.length)

      val batch1 = new CachedVeloxBatch(batch.asInstanceOf[VeloxColumnarBatch], schema)
      val serializer = new KryoSerializer(new SparkConf())
      val instance = serializer.newInstance()
      val batch2 = instance.deserialize[CachedVeloxBatch](instance.serialize(batch1))

      Range(0, variants.length)
        .foreach {
          i =>
            variants.apply(i).getString(0) == batch2.veloxBatch.getRow(i).getString(0)
        }
      println(batch2)
    }
  }
}
