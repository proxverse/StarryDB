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

import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{
  BroadcastMode,
  BroadcastPartitioning,
  Partitioning
}
import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch
import org.apache.spark.sql.execution.columnar.jni.{NativeColumnarVector, NativeQueryContext}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.SparkFatalException
import org.apache.spark._
import org.apache.spark.internal.Logging

import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.concurrent.Promise
import scala.concurrent.duration.NANOSECONDS
import scala.util.control.NonFatal

class CloseableColumnBatchIterator(
    itr: Iterator[ColumnarBatch],
    pipelineTime: Option[SQLMetric] = None)
    extends Iterator[ColumnarBatch]
    with Logging {
  var cb: ColumnarBatch = _
  var scanTime = 0L

  override def hasNext: Boolean = {
    val beforeTime = System.nanoTime()
    val res = itr.hasNext
    scanTime += System.nanoTime() - beforeTime
    if (!res) {
      pipelineTime.foreach(t => t += TimeUnit.NANOSECONDS.toMillis(scanTime))
    }
    res
  }
  override def next(): ColumnarBatch = {
    val beforeTime = System.nanoTime()
    closeCurrentBatch()
    cb = itr.next()
    scanTime += System.nanoTime() - beforeTime
    cb
  }

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }
}

object ColumnarBroadcastExchangeExec {
  def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): VeloxBuildSideRelation = {
    val batches = child
      .executeColumnar()
      .map { batch =>
        val veloxColumnarBatch = batch.asInstanceOf[VeloxColumnarBatch]
        numOutputRows.add(veloxColumnarBatch.numRows())
        val vector = veloxColumnarBatch.rowVector()
        val str = vector.serialize()
        dataSize.add(str.length)
        veloxColumnarBatch.close()
        vector.close()
        str
      }
      .collect
    VeloxBuildSideRelation(mode, child.output, batches)
  }
}

/**
 *  特殊的 columnar 类型
 * @param mode
 * @param child
 */
case class ColumnarBroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan)
    extends BroadcastExchangeLike {

  @transient override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  @transient
  lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future
  @transient
  private lazy val maxBroadcastRows = mode match {
    case HashedRelationBroadcastMode(key, _)
        // NOTE: LongHashedRelation is used for single key with LongType. This should be kept
        // consistent with HashedRelation.apply.
        if !(key.length == 1 && key.head.dataType == LongType) =>
      // Since the maximum number of keys that BytesToBytesMap supports is 1 << 29,
      // and only 70% of the slots can be used before growing in UnsafeHashedRelation,
      // here the limitation should not be over 341 million.
      (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong
    case _ => 512000000
  }

  @transient
  override lazy val relationFuture: java.util.concurrent.Future[broadcast.Broadcast[Any]] = {
    val queryContext = NativeQueryContext.get
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session,
      BroadcastExchangeExec.executionContext) {
      queryContext.attachCurrentThread()
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(
          runId.toString,
          s"broadcast exchange (runId $runId)",
          interruptOnCancel = true)
        val beforeCollect = System.nanoTime()

        // this created relation ignore HashedRelationBroadcastMode isNullAware, because we cannot
        // get child output rows, then compare the hash key is null, if not null, compare the
        // isNullAware, so gluten will not generate HashedRelationWithAllNullKeys or
        // EmptyHashedRelation, this difference will cause performance regression in some cases
        val relation = ColumnarBroadcastExchangeExec.createBroadcastRelation(
          mode,
          child,
          longMetric("numOutputRows"),
          longMetric("dataSize"))
        val beforeBroadcast = System.nanoTime()

        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeCollect)

        // Broadcast the relation
        val broadcasted = sparkContext.broadcast(relation.asInstanceOf[Any])
        longMetric("broadcastTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBroadcast)

        // Update driver metrics
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

        promise.success(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(
            new OutOfMemoryError(
              "Not enough memory to build and broadcast the table to all " +
                "worker nodes. As a workaround, you can either disable broadcast by setting " +
                s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
                s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
              .initCause(oe.getCause))
          promise.failure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.failure(ex)
          throw ex
        case e: Throwable =>
          promise.failure(e)
          throw e
      } finally {
        queryContext.detachCurrentThread()
      }
    }
  }

  override val runId: UUID = UUID.randomUUID

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = {
    ColumnarBroadcastExchangeExec(mode.canonicalized, child.canonicalized)
  }

  def doValidate(): Boolean = mode match {
    case _: HashedRelationBroadcastMode =>
      true
    case _ =>
      // IdentityBroadcastMode not supported. Need to support BroadcastNestedLoopJoin first.
      false
  }

  override def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "ColumnarBroadcastExchange does not support the execute() code path.")
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(
          s"""
             |Could not execute broadcast in $timeout secs.
             |You can increase the timeout for broadcasts via
             |${SQLConf.BROADCAST_TIMEOUT.key} or disable broadcast join
             |by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1
            """.stripMargin,
          ex)
    }
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): ColumnarBroadcastExchangeExec =
    copy(child = newChild)

  // Ported from BroadcastExchangeExec
  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    BroadcastBuildSideRDD(sparkContext, doExecuteBroadcast[VeloxBuildSideRelation]())
  }
}

private final case class BroadcastBuildSideRDDPartition(index: Int) extends Partition

case class BroadcastBuildSideRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[VeloxBuildSideRelation],
    numPartitions: Int = -1)
    extends RDD[ColumnarBatch](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    if (numPartitions < 0) {
      throw new RuntimeException(s"Invalid number of partitions: $numPartitions.")
    }
    Array.tabulate(numPartitions)(i => BroadcastBuildSideRDDPartition(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    new CloseableColumnBatchIterator(broadcasted.value.deserialized)

  }
}

case class VeloxBuildSideRelation(
    mode: BroadcastMode,
    output: Seq[Attribute],
    batches: Array[Array[Byte]]) {

  def deserialized: Iterator[ColumnarBatch] = {
    val structType = StructType.fromAttributes(output)
    val numRows = SQLConf.get.columnBatchSize
    val rowIterator = batches.iterator
    if (rowIterator.hasNext) {
      val res = new Iterator[ColumnarBatch] {
        private var elapse: Long = 0

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          var rowCount = 0
          val row = rowIterator.next()
          val start = System.nanoTime()
          val cb = VeloxColumnarBatch.createFromJson(
            row,
            structType)
          elapse += System.nanoTime() - start
          rowCount += cb.numRows()
          cb
        }
      }
      new CloseableColumnBatchIterator(res)
    } else {
      Iterator.empty
    }
  }
}
