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

import java.util.function.Supplier
import scala.concurrent.Future
import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{CompressedMapStatus, MapStatus}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  BoundReference,
  UnsafeProjection,
  UnsafeRow
}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.exchange.{
  ENSURE_REQUIREMENTS,
  Exchange,
  ShuffleExchangeExec,
  ShuffleExchangeLike,
  ShuffleOrigin
}
import org.apache.spark.sql.execution.metric.{
  SQLMetric,
  SQLMetrics,
  SQLShuffleReadMetricsReporter,
  SQLShuffleWriteMetricsReporter
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
case class ColumnarPartitionedOutputExec(
    override val outputPartitioning: HashPartitioning,
    child: SparkPlan)
    extends UnaryExecNode
    with ColumnarSupport {

  override def supportsColumnar: Boolean =
    true

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[ColumnarPartitionedOutputExec]
  }

  override def hashCode(): Int = super.hashCode()

  override def withNewChildInternal(newChild: SparkPlan): ColumnarPartitionedOutputExec =
    new ColumnarPartitionedOutputExec(outputPartitioning, newChild)

  override def collectPartitions(): Seq[FilePartition] = {
    child.asInstanceOf[ColumnarSupport].collectPartitions()
  }

  override def makePlanInternal(operations: NativePlanBuilder): Unit = {
    if (outputPartitioning.numPartitions == 1) {
      operations.partitionedOutput(
        null,
        outputPartitioning.numPartitions)
    } else {
      operations.partitionedOutput(
        outputPartitioning.expressions.map(toNativeExpressionJson).toArray,
        outputPartitioning.numPartitions)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  override def output: Seq[Attribute] =
    Seq(
      AttributeReference("partition", IntegerType, false)(),
      AttributeReference("binary", StringType, false)())
}
