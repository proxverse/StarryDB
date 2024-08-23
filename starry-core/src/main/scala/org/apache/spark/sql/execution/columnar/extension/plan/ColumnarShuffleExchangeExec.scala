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
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{CompressedMapStatus, MapStatus}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, Exchange, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.shuffle.{AddBatch, StarryShuffleConstants}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordComparator}
import org.apache.spark.util.random.XORShiftRandom

import java.util.concurrent.TimeUnit

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
case class ColumnarShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS,
    override val output: Seq[Attribute])
    extends ShuffleExchangeLike {

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics
      .createMetric(sparkContext, "number of partitions")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "Exchange"

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  @transient lazy val inputColumnarRDD: RDD[ColumnarBatch] = child.executeColumnar()

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient
  override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[InternalRow] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  lazy val shuffleServices = StarryEnv.get.shuffleManagerMaster.shuffleServices()

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    val dep = ColumnarShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      outputPartitioning,
      serializer,
      writeMetrics,
      shuffleServices)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  /**
   * Caches the created ShuffleRowRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledRowRDD = null
  private var cachedColumnarShuffleRDD: ShuffledColumnarRDD = null

  override def supportsColumnar: Boolean = false

  protected override def doExecute(): RDD[InternalRow] = {
    // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledRowRDD(shuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedColumnarShuffleRDD == null) {
      cachedColumnarShuffleRDD =
        new ShuffledColumnarRDD(shuffleDependency, readMetrics, output, shuffleServices)
    }
    cachedColumnarShuffleRDD
  }
  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarShuffleExchangeExec =
    copy(child = newChild)
}

object ColumnarShuffleExchangeExec {

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      shuffleServices: Array[(String, RpcAddress)])
    : ShuffleDependency[Int, InternalRow, InternalRow] = {

    val rddWithPartitionIds = rdd.mapPartitionsWithIndexInternal((_, iter) => {
      iter.map { row =>
        (row.getInt(0), row)
      }
    }, isOrderSensitive = false)

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(newPartitioning.numPartitions),
        serializer,
        shuffleWriterProcessor =
          createColumnarShuffleWriteProcessor(writeMetrics, shuffleServices))
    dependency
  }

  /**
   * Create a customized [[ShuffleWriteProcessor]] for SQL which wrap the default metrics reporter
   * with [[SQLShuffleWriteMetricsReporter]] as new reporter for [[ShuffleWriteProcessor]].
   */
  def createColumnarShuffleWriteProcessor(
      metrics: Map[String, SQLMetric],
      shuffleServices: Array[(String, RpcAddress)]): ShuffleWriteProcessor = {
    new ShuffleWriteProcessor {
      override protected def createMetricsReporter(
          context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }
      override def write(
          rdd: RDD[_],
          dep: ShuffleDependency[_, _, _],
          mapId: Long,
          context: TaskContext,
          partition: Partition): MapStatus = {
        val reporter = createMetricsReporter(context)
        val rpcs = shuffleServices.map { address =>
          SparkEnv.get.rpcEnv.setupEndpointRef(address._2, address._1)
        }
        val partitions = rpcs.length
        rdd
          .iterator(partition, context)
          .asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
          .foreach { tp =>
            val partition = tp._1.asInstanceOf[Int]
            val start = System.nanoTime()
            val bytes = tp._2.asInstanceOf[InternalRow].getBinary(1)
            reporter.incBytesWritten(bytes.size)
            rpcs
              .apply(partition % partitions)
              .send(
                AddBatch(dep.shuffleId, partition, bytes))
            reporter.incWriteTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
          }
        new CompressedMapStatus(
          SparkEnv.get.blockManager.blockManagerId,
          Array.fill(dep.partitioner.numPartitions) { 1L },
          mapId)
      }
    }
  }
}
