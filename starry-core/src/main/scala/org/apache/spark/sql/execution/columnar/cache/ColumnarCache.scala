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
package org.apache.spark.sql.execution.columnar.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.columnar.{
  CachedBatch,
  CachedBatchSerializer,
  SimpleMetricsCachedBatch
}
import org.apache.spark.sql.execution.columnar.{NoCloseColumnVector, VeloxColumnarBatch}
import org.apache.spark.sql.execution.columnar.extension.plan.RowToVeloxColumnarExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.KnownSizeEstimation

import scala.collection.JavaConverters._

case class CachedVeloxBatch(veloxBatch: VeloxColumnarBatch)
    extends CachedBatch
    with AutoCloseable
    with KnownSizeEstimation {
  override def close(): Unit = {
    veloxBatch.close()
  }

  override def numRows: Int = {
    veloxBatch.numRows()
  }

  override def sizeInBytes: Long = {
//    GlutenColumnBatchUtils.estimateFlatSize(veloxBatch.getObjectPtr)
    100
  }

  override def estimatedSize: Long = {
    100
  }
}

case class DefaultCachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)
    extends SimpleMetricsCachedBatch

/** The default implementation of CachedBatchSerializer. */
class CachedVeloxBatchSerializer extends CachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val structType = StructType.fromAttributes(schema)

    input.map { batch =>
      val veloxColumnarBatch = batch.asInstanceOf[VeloxColumnarBatch]
      veloxColumnarBatch.setSchema(structType)
      val cacheBatch = CachedVeloxBatch(veloxColumnarBatch.copy())
      veloxColumnarBatch.close()
      cacheBatch
    }

  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    convertForCacheInternal(input, schema, batchSize, useCompression)
  }

  def convertForCacheInternal(
      input: RDD[InternalRow],
      output: Seq[Attribute],
      batchSize: Int,
      useCompression: Boolean): RDD[CachedBatch] = {
    val structType = StructType.fromAttributes(output)
    input.mapPartitionsInternal { itr =>
      RowToVeloxColumnarExec
        .toVeloxBatchIterator(
          new SQLMetric("numInputRows"),
          new SQLMetric("numInputRows"),
          batchSize,
          structType,
          itr)
        .map { batch =>
          // tobe remove, when RowToVeloxColumnarExec can specified memory pool
          val veloxColumnarBatch = batch.asInstanceOf[VeloxColumnarBatch]
          veloxColumnarBatch.setSchema(structType)
          val cacheBatch = CachedVeloxBatch(veloxColumnarBatch.copy())
          veloxColumnarBatch.close()
          cacheBatch
        }
    }
  }

  override def supportsColumnarOutput(schema: StructType): Boolean =
    schema.fields.forall(f =>
      f.dataType match {
        // More types can be supported, but this is to match the original implementation that
        // only supported primitive types "for ease of review"
        case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
            DoubleType | TimestampType | DateType | StringType =>
          true
        case x: ArrayType =>
          true
        case _ =>
          false
    })

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(if (!conf.offHeapColumnVectorEnabled) {
      classOf[OnHeapColumnVector].getName
    } else {
      classOf[OffHeapColumnVector].getName
    }))

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    // Find the ordinals and data types of the requested columns.
    val (requestedColumnIndices, requestedColumnDataTypes) =
      selectedAttributes.map { a =>
        cacheAttributes
          .map(_.name)
          .indexOf(a.name) -> a.dataType // use name to match to void exprid change when has join
      }.unzip
    val structType = StructType.fromAttributes(selectedAttributes)
    input.mapPartitionsInternal { itr =>
      itr
        .map { batch =>
          val veloxBatch = batch.asInstanceOf[CachedVeloxBatch].veloxBatch
          val veloxColumnarBatch = veloxBatch.getColumns
          val vectors = requestedColumnIndices
            .map(veloxColumnarBatch.apply)
            .map(c => new NoCloseColumnVector(c).asInstanceOf[ColumnVector])
            .toArray
          val newBatch = new VeloxColumnarBatch(vectors, veloxBatch.numRows())
          newBatch
        }
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    // Find the ordinals and data types of the requested columns.
    val (requestedColumnIndices, requestedColumnDataTypes) =
      selectedAttributes.map { a =>
        cacheAttributes.map(_.name).indexOf(a.name) -> a.dataType
      }.unzip
    val structType = StructType.fromAttributes(selectedAttributes)

    input.mapPartitionsInternal { itr =>
      val projection = UnsafeProjection.create(structType)
      itr
        .map { batch =>
          val veloxBatch = batch.asInstanceOf[CachedVeloxBatch].veloxBatch
          val GlutenVeloxColumnarBatch = veloxBatch.getColumns
          val vectors = requestedColumnIndices
            .map(GlutenVeloxColumnarBatch.apply)
            .map(c => new NoCloseColumnVector(c).asInstanceOf[ColumnVector])
            .toArray
          val newBatch = new VeloxColumnarBatch(vectors, veloxBatch.numRows())
          newBatch
        }
        .flatMap(_.rowIterator().asScala)
        .map(projection.apply)
    }
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] =
    throw new UnsupportedOperationException()
}
