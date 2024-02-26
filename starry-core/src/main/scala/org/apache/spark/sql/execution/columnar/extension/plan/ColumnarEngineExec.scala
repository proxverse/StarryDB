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

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.columnar.extension.ColumnarExecutionRDD
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import java.util
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

object ColumnarEngineExec {
  val transformStageCounter = new AtomicInteger(0)
}
case class ColumnarEngineExec(child: SparkPlan)(
    val columnarStageId: Int)
    extends SparkPlan
    with ColumnarToRowTransition {

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"))

  override def nodeName: String = s"WholeStageCodegen Columnar ($columnarStageId)"

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($columnarStageId) ",
      false,
      maxFields,
      printNodeId,
      indent)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    prepareVectorRDD()
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    prepareVectorRDD().mapPartitionsInternal { batches =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      batches.flatMap { batch =>
        numInputBatches += 1
        numOutputRows += batch.numRows()
        batch.rowIterator().asScala.map(toUnsafe)
      }
    }
  }

  @transient
  private implicit lazy val formats = Serialization.formats(NoTypeHints)
  def prepareVectorRDD(): RDD[ColumnarBatch] = {
    val partitions = child.asInstanceOf[ColumnarSupport]
    val builder = new NativePlanBuilder
    partitions.makePlan(builder)
    val planJson = builder.builderAndRelease()
    val nodeMetrics = new util.HashMap[String, (String, Map[String, SQLMetric])]()
    partitions.collectMetrics(nodeMetrics)
    val map1 = sparkContext.conf.getAllWithPrefix("spark.sql").toMap
    new ColumnarExecutionRDD(
      sparkContext,
      planJson,
      partitions.columnarInputRDDs.map(_._1).toArray,
      partitions.columnarInputRDDs.map(_._2).toArray,
      child.output,
      nodeMetrics.asScala.toMap,
      map1,
      columnarStageId)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarEngineExec =
    copy(child = newChild)(columnarStageId)

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    children.head.executeBroadcast()
  }

}
