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

package org.apache.spark.sql.execution.columnar.extension.plan

import org.apache.spark.{TaskContext, broadcast}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.columnar.extension.plan.RowToVeloxColumnarExec.toVeloxBatchIterator
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
object RowToVeloxColumnarExec {
  def toVeloxBatchIterator(
      numInputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      batchSize: Int,
      localSchema: StructType,
      rowIterator: Iterator[InternalRow]): Iterator[ColumnarBatch] = {
    if (rowIterator.hasNext) {
      val res = new Iterator[ColumnarBatch] {
        private val converters = new VeloxRowToColumnConverter(localSchema)
        private var last_cb: ColumnarBatch = null
        private var elapse: Long = 0

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          val cb = ColumnBatchUtils.createWriterableColumnBatch(batchSize, localSchema)
          var rowCount = 0
          while (rowCount < batchSize && rowIterator.hasNext) {
            val row = rowIterator.next()
            val start = System.nanoTime()
            converters.convert(
              row,
              cb.asInstanceOf[VeloxColumnarBatch]
                .getColumns
                .map(_.asInstanceOf[WritableColumnVector]))
            elapse += System.nanoTime() - start
            rowCount += 1
          }
          cb.setNumRows(rowCount)
          numInputRows += rowCount
          numOutputBatches += 1
          last_cb = cb
          last_cb
        }
      }
      new CloseableColumnBatchIterator(res)
    } else {
      Iterator.empty
    }
  }
}

/**
 * Provides a common executor to translate an [[RDD]] of [[InternalRow]] into an [[RDD]] of
 * [[ColumnarBatch]]. This is inserted whenever such a transition is determined to be needed.
 *
 * This is similar to some of the code in ArrowConverters.scala and
 * [[org.apache.spark.sql.execution.arrow.ArrowWriter]]. That code is more specialized to convert
 * [[InternalRow]] to Arrow formatted data, but in the future if we make [[OffHeapColumnVector]]
 * internally Arrow formatted we may be able to replace much of that code.
 *
 * This is also similar to
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.populate()]] and
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.toBatch()]] toBatch is only ever
 * called from tests and can probably be removed, but populate is used by both Orc and Parquet to
 * initialize partition and missing columns. There is some chance that we could replace populate
 * with [[RowToColumnConverter]], but the performance requirements are different and it would only
 * be to reduce code.
 */
case class RowToVeloxColumnarExec(child: SparkPlan)
    extends RowToColumnarTransition
    with UnaryExecNode {

  @transient override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of intput rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    child.execute().mapPartitions { rowIterator =>
      toVeloxBatchIterator(numInputRows, numOutputBatches, numRows, localSchema, rowIterator)
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = true

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): RowToVeloxColumnarExec =
    copy(child = newChild)

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }
}
