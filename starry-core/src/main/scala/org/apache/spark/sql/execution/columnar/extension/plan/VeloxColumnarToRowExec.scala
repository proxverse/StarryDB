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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class VeloxColumnarToRowExec(child: SparkPlan) extends ColumnarToRowTransition {
  private val LOG = LoggerFactory.getLogger(classOf[VeloxColumnarToRowExec])
  @transient override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert"))
  override def nodeName: String = "VeloxColumnarToRowExec"

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")

    new ColumnarToRowRDD(
      sparkContext,
      child.executeColumnar(),
      this.output,
      numOutputRows,
      numInputBatches,
      convertTime)
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected def withNewChildInternal(newChild: SparkPlan): VeloxColumnarToRowExec =
    copy(child = newChild)
}

class ColumnarToRowRDD(
    @transient sc: SparkContext,
    rdd: RDD[ColumnarBatch],
    output: Seq[Attribute],
    numOutputRows: SQLMetric,
    numInputBatches: SQLMetric,
    convertTime: SQLMetric)
    extends RDD[InternalRow](sc, Seq(new OneToOneDependency(rdd))) {

  private val cleanedF = sc.clean(f)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    cleanedF(firstParent[ColumnarBatch].iterator(split, context))
  }

  private def f: Iterator[ColumnarBatch] => Iterator[InternalRow] = { batches =>
    var closed = false

    if (batches.isEmpty) {
      Iterator.empty
    } else {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
        val localOutput = output
        val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
        override def hasNext: Boolean = {
          val hasNext = batches.hasNext
          if (!hasNext && !closed) {
            closed = true
          }
          hasNext
        }

        override def next(): Iterator[InternalRow] = {
          val batch = batches.next()
          numInputBatches += 1
          numOutputRows += batch.numRows()
          batch.rowIterator().asScala.map(toUnsafe)
        }
      }
      res.flatten
    }
  }

  override def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions
}
