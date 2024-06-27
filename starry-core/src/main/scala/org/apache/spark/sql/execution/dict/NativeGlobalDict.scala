package org.apache.spark.sql.execution.dict

import org.apache.spark.broadcast.{Broadcast, TorrentBroadcast}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.extension.plan.VeloxRowToColumnConverter
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.ThreadUtils

import scala.concurrent.ExecutionContext

case class NativeGlobalDict(@transient dataFrame: DataFrame) extends ColumnDict with Logging {

  private val schema: StructType = dataFrame.schema
  override lazy val dataType = schema.head.dataType

  // 这里为了不污染原来字典的的执行计划
  @transient lazy val broadcastVectorBytes: Broadcast[IDict] = {
    NativeGlobalDict.executionContext
      .submit { () =>
        val valueCount = dataFrame.count().toInt
        val dataType = dataFrame.schema.head.dataType
        val schema = StructType(Seq(StructField("dict", dataType)))
        val converter = VeloxRowToColumnConverter.getConverterForType(dataType, false)
        val batch = ColumnBatchUtils.createWriterableColumnBatch(valueCount, schema)
        try {
          val start = System.currentTimeMillis()
          val rows =
            SQLExecution.withNewExecutionId(dataFrame.queryExecution, Some("collectInternal")) {
              QueryExecution.withInternalError(s"""The collectInternal action failed.""") {
                dataFrame.queryExecution.executedPlan.resetMetrics()
                dataFrame.queryExecution.executedPlan.executeCollect()
              }
            }
          rows.foreach(row =>
            converter.append(row, 0, batch.column(0).asInstanceOf[WritableColumnVector]))
          batch.setNumRows(valueCount)
          log.info(s"Create dict broadcast take time ${System.currentTimeMillis() - start} ms")
          val rowVector = batch.asInstanceOf[VeloxColumnarBatch].rowVector()
          val bytes = rowVector.serialize()
          rowVector.close()
          SparkSession.active.sparkContext.broadcast(Dict(bytes).asInstanceOf[IDict])
        } finally {
          batch.close()
        }
      }
      .get()
  }

  override def cleanup(): Unit = broadcastVectorBytes.destroy(false)

  override val broadcastID: Long = broadcastVectorBytes.id

  override val broadcastNumBlocks: Int =
    broadcastVectorBytes.asInstanceOf[TorrentBroadcast[Dict]].numBlocks
}

object NativeGlobalDict {
  val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool(
      "native dict",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
