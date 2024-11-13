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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer, SimpleMetricsCachedBatch}
import org.apache.spark.sql.execution.columnar.extension.MetricsUpdater.applyMetrics
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, NoCloseColumnVector, VeloxColumnarBatch}
import org.apache.spark.sql.execution.columnar.extension.plan.{CloseableColumnBatchIterator, RowToVeloxColumnarExec, VeloxRowToColumnConverter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._

case class DefaultCachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)
    extends SimpleMetricsCachedBatch

/** The default implementation of CachedBatchSerializer. */
class CachedVeloxBatchSerializer extends CachedBatchSerializer with Logging{
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    if (storageLevel.useDisk && storageLevel.useMemory) {
      logWarning(s"columnar cache only support one storage ${Thread.currentThread().getStackTrace.mkString(",")}")
    }
    val structType = StructType.fromAttributes(schema)
    val batchSize = conf.columnBatchSize

    input.mapPartitions { itr =>
      val batches = itr.map(batch => copyBatch(batchSize, structType, batch))
      if (storageLevel.useDisk) {
        new CloseableColumnBatchIterator[CachedVeloxBatch](batches)
      } else {
        batches
      }
    }

  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    if (storageLevel.useDisk && storageLevel.useMemory) {
      logWarning(s"columnar cache only support one storage ${Thread.currentThread().getStackTrace.mkString(",")}")
    }
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    convertForCacheInternal(input, schema, batchSize, useCompression, storageLevel)
  }

  def convertForCacheInternal(
      input: RDD[InternalRow],
      output: Seq[Attribute],
      batchSize: Int,
      useCompression: Boolean,
      storageLevel: StorageLevel): RDD[CachedBatch] = {
    val structType = StructType.fromAttributes(output)
    input.mapPartitionsInternal { itr =>
      val batches = RowToVeloxColumnarExec
        .toVeloxBatchIterator(
          new SQLMetric("numInputRows"),
          new SQLMetric("numInputRows"),
          new SQLMetric("numInputRows"),
          batchSize,
          structType,
          itr)
        .map { batch =>
          copyBatch(batchSize, structType, batch)
        }
      if (storageLevel.useDisk) {
        new CloseableColumnBatchIterator[CachedVeloxBatch](batches)
      } else {
        batches
      }
    }
  }

  private def copyBatch(batchSize: Int, structType: StructType, batch: ColumnarBatch) = {
    try {
      if (batch.numRows() < batchSize && batch.numRows() > 0) {
        val converters = new VeloxRowToColumnConverter(structType)
        val cb = ColumnBatchUtils.createWriterableColumnBatch(batch.numRows(), structType)
        Range(0, batch.numRows()).foreach { rowIndex =>
          converters.convert(
            batch.getRow(rowIndex),
            cb.asInstanceOf[VeloxColumnarBatch]
              .getColumns
              .map(_.asInstanceOf[WritableColumnVector]))
        }
        cb.setNumRows(batch.numRows())
        new CachedVeloxBatch(cb.asInstanceOf[VeloxColumnarBatch], structType)
      } else {
        val veloxColumnarBatch = batch.asInstanceOf[VeloxColumnarBatch]
        val cacheBatch = new CachedVeloxBatch(veloxColumnarBatch.copy(structType), structType)
        cacheBatch
      }
    } catch {
      case e =>
        val veloxColumnarBatch = batch.asInstanceOf[VeloxColumnarBatch]
        val cacheBatch = new CachedVeloxBatch(veloxColumnarBatch.copy(structType), structType)
        cacheBatch
    } finally {
      batch.close()
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
      val batches = new CloseableColumnBatchIterator[ColumnarBatch](itr
        .map { batch =>
          val veloxBatch = batch.asInstanceOf[CachedVeloxBatch].veloxBatch
          val veloxColumnarBatch = veloxBatch.getColumns
          val vectors = requestedColumnIndices
            .map(veloxColumnarBatch.apply)
            .map(c => new NoCloseColumnVector(c).asInstanceOf[ColumnVector])
            .toArray
          val newBatch = new VeloxColumnarBatch(vectors, veloxBatch.numRows())
          newBatch
        })
      TaskContext.get().addTaskCompletionListener[Unit] { t =>
        batches.close()
      }
      batches
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
          val columns = veloxBatch.getColumns
          val vectors = requestedColumnIndices
            .map(columns.apply)
            .map(c => NoCloseColumnVector(c).asInstanceOf[ColumnVector])
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
