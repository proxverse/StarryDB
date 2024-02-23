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

package org.apache.spark.sql.execution.columnar.extension

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.columnar.extension.MetricsUpdater.applyMetrics
import org.apache.spark.sql.execution.columnar.extension.plan.CloseableColumnBatchIterator
import org.apache.spark.sql.execution.columnar.extension.vector.ColumnarBatchInIterator
import org.apache.spark.sql.execution.columnar.jni.NativeColumnarExecution
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark._
import org.json4s
import org.json4s.{NoTypeHints, _}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

class ZippedPartitionsPartition(idx: Int, @transient private val rdds: Seq[RDD[_]])
    extends Partition {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))

  def partitions: Seq[Partition] = partitionValues
}

class ColumnarExecutionRDD(
    @transient private val sc: SparkContext,
    planJson: String,
    var nodeIds: Array[String],
    var rdds: Array[RDD[ColumnarBatch]],
    outputAttributes: Seq[Attribute],
    nodeMetrics: Map[String, (String, Map[String, SQLMetric])],
    useTableCacheMemoryPool: Boolean,
    confMap: Map[String, String],
    columnarStageId: Long)
    extends RDD[ColumnarBatch](sc, rdds.map(x => new OneToOneDependency(x))) {

  @transient
  private implicit lazy val formats = Serialization.formats(NoTypeHints)

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
    val inputIterators: Seq[Iterator[ColumnarBatch]] = (rdds zip partitions).map {
      case (rdd, partition) => rdd.iterator(partition, context)
    }
    val execution = if (useTableCacheMemoryPool) {
      new NativeColumnarExecution(outputAttributes.toList.asJava, "table_cache")
    } else {
      new NativeColumnarExecution(
        outputAttributes.toList.asJava,
        s"${TaskContext.get().stageId()}_${TaskContext.getPartitionId()}" +
          s"_${TaskContext.get().taskAttemptId()}")
    }
    val columnarNativeIterator =
      inputIterators.map { iter =>
        new ColumnarBatchInIterator(iter.asJava)
      }.toArray

    val finalMap = confMap ++ Map(
      "task_id" -> s"${columnarStageId}_${TaskContext.get().taskAttemptId()}")
    val confStr = Serialization.write(finalMap)

    execution.init(planJson, nodeIds, columnarNativeIterator, confStr)

    TaskContext.get().addTaskCompletionListener[Unit] { t =>
      val metrics = parse(execution.getMetrics)
      applyMetrics(nodeMetrics, metrics)
      execution.close()
    }
    val iter = new Iterator[ColumnarBatch] {

      override def hasNext: Boolean = {
        val hasNext = execution.hasNext
        hasNext
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        val next1 = execution.next()
        next1
      }
    }
    new CloseableColumnBatchIterator(iter)
  }

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}")
    }
    Array.tabulate[Partition](numParts) { i =>
      new ZippedPartitionsPartition(i, rdds)
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }

}

object MetricsUpdater {
  val pattern: Regex = "wallNanos:\\s*(\\d+)".r
  val text = "count: 4, wallNanos: 518708, cpuNanos: 510249"

  val wallNanosValue: Option[String] = pattern.findFirstMatchIn(text).map(_.group(1))

  def applyMetrics(
      nodeMetrics: Map[String, (String, Map[String, SQLMetric])],
      metrics: json4s.JValue): Unit = {
    val seqTry = Try(metrics.values.asInstanceOf[List[Map[String, Any]]])
    seqTry.get.foreach { nodeStatus =>
      val nodeId = nodeStatus.apply("planNodeId").toString
      val tuple = nodeMetrics.apply(nodeId)
      tuple._2.foreach { tp =>
        tp._1 match {
          case "peakMemoryBytes" => tp._2.add(nodeStatus.apply("peakMemoryBytes").toString.toLong)
          case "cpuWallTiming" =>
            tp._2.add(
              pattern
                .findFirstMatchIn(nodeStatus.apply("cpuWallTiming").toString)
                .map(_.group(1))
                .get
                .toLong / 1000000)
          case "numInputRows" => tp._2.add(nodeStatus.apply("inputRows").toString.toLong)
          case "numOutputRows" => tp._2.add(nodeStatus.apply("outputRows").toString.toLong)
          case _ =>
        }
      }
    }
  }
}
