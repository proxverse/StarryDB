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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch.createFromBytes
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConverter
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.shuffle.{
  BatchMessage,
  FetchBatch,
  FetchStreamingBatch,
  QueryBatch,
  RemoveShufflePartition,
  StreamingBatchMessage
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.TimeUnit

/**
 * This is a specialized version of [[org.apache.spark.rdd.ShuffledRDD]] that is optimized for
 * shuffling rows instead of Java key-value pairs. Note that something like this should eventually
 * be implemented in Spark core, but that is blocked by some more general refactorings to shuffle
 * interfaces / internals.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`),
 * and an array of [[ShufflePartitionSpec]] as input arguments.
 *
 * The `dependency` has the parent RDD of this RDD, which represents the dataset before shuffle
 * (i.e. map output). Elements of this RDD are (partitionId, Row) pairs.
 * Partition ids should be in the range [0, numPartitions - 1].
 * `dependency.partitioner` is the original partitioner used to partition
 * map output, and `dependency.partitioner.numPartitions` is the number of pre-shuffle partitions
 * (i.e. the number of partitions of the map output).
 */
class StreamingShuffledColumnarRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec],
    attributes: Seq[Attribute],
    shuffleServices: Array[(String, RpcAddress)],
    isMppMode: Boolean)
    extends RDD[ColumnarBatch](dependency.rdd.context, Nil) {

  def this(
      dependency: ShuffleDependency[Int, InternalRow, InternalRow],
      metrics: Map[String, SQLMetric],
      attributes: Seq[Attribute],
      shuffleServices: Array[(String, RpcAddress)],
      isMppMode: Boolean) = {
    this(
      dependency,
      metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)),
      attributes,
      shuffleServices,
      isMppMode)
  }

  dependency.rdd.context.setLocalProperty(
    SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY,
    SQLConf.get.fetchShuffleBlocksInBatch.toString)

  override def getDependencies: Seq[Dependency[_]] =
    if (isMppMode) { super.getDependencies } else { List(dependency) }

  override val partitioner: Option[Partitioner] =
    if (partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])) {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
      if (indices.toSet.size == partitionSpecs.length) {
        Some(new CoalescedPartitioner(dependency.partitioner, indices))
      } else {
        None
      }
    } else {
      None
    }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      ShuffledRowRDDPartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val structType = StructType.fromAttributes(
      attributes.map(e => e.withName(ExpressionConverter.toNativeAttrIdName(e))))
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    val rpcs = shuffleServices.map { address =>
      SparkEnv.get.rpcEnv.setupEndpointRef(address._2, address._1)
    }
    val partitions = rpcs.length
    val reader = split.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        TaskContext
          .get()
          .addTaskCompletionListener[Unit](_ =>
            Range(startReducerIndex, endReducerIndex).foreach { reduceId =>
              rpcs
                .apply(reduceId % partitions)
                .send(RemoveShufflePartition(dependency.shuffleId, reduceId))
          })
        Range(startReducerIndex, endReducerIndex).flatMap { reduceId =>
          val ref = rpcs
            .apply(reduceId % partitions)
          new ColumnarBatchIterator(
            ref,
            reduceId,
            dependency.shuffleId,
            sqlMetricsReporter,
            structType)

        }
      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        throw new UnsupportedOperationException()

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        throw new UnsupportedOperationException()

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        throw new UnsupportedOperationException()

    }
    reader.iterator
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dependency = null
  }
}

class ColumnarBatchIterator(
    ref: RpcEndpointRef,
    startReducerIndex: Int,
    shuffleId: Int,
    sqlMetricsReporter: SQLShuffleReadMetricsReporter,
    structType: StructType)
    extends Iterator[ColumnarBatch] {

  private var buffer: java.util.ArrayList[Array[Byte]] = new java.util.ArrayList[Array[Byte]]()
  private var isFinish = false

  // Fetch the next batch if necessary
  private def fetchNext(): Unit = {
    while (buffer.isEmpty && !isFinish) {
      val startFetchWait = System.nanoTime()
      val message =
        ref.askSync[StreamingBatchMessage](FetchStreamingBatch(shuffleId, startReducerIndex))
      sqlMetricsReporter.incFetchWaitTime(
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait))
      isFinish = message.isFinish
      if (message.batch.nonEmpty) {
        message.batch.foreach(buffer.add)
      } else if (!isFinish) {
        // 如果拿到空批次但还没结束，等待一段时间后重试
        // 这里的等待时间需要根据实际情况调整
        Thread.sleep(5) // 等待时间，比如100毫秒
      }
    }
  }

  override def hasNext: Boolean = {
    fetchNext()
    buffer.size() > 0
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    val batchBytes = buffer.remove(0)
    convertBytesToColumnarBatch(batchBytes)
  }

  // 实现从字节转换到ColumnarBatch的逻辑
  private def convertBytesToColumnarBatch(batchBytes: Array[Byte]): ColumnarBatch = {
    sqlMetricsReporter.incRemoteBytesRead(batchBytes.length)
    val startConversionTime = System.nanoTime()
    // 假设createFromBytes是一个存在的方法来从字节创建ColumnarBatch
    val batch = createFromBytes(batchBytes, structType, 1).asInstanceOf[ColumnarBatch]
    sqlMetricsReporter.incFetchWaitTime(
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startConversionTime))
    sqlMetricsReporter.incRecordsRead(batch.numRows())
    batch
  }
}
