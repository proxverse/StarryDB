package org.apache.spark.sql.execution.dict

import org.apache.spark.broadcast.{Broadcast, TorrentBroadcast}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.extension.plan.VeloxRowToColumnConverter
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class NativeGlobalDict(@transient dataFrame: DataFrame) extends ColumnDict with Logging {

  private val schema: StructType = dataFrame.schema
  override lazy val dataType = schema.head.dataType

  @transient lazy val broadcastVectorBytes: Broadcast[IDict] = {
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
      SparkSession.active.sparkContext.broadcast(Dict(bytes))
    } finally {
      batch.close()
    }
  }

  override def cleanup(): Unit = broadcastVectorBytes.destroy(false)

  override val broadcastID: Long = broadcastVectorBytes.id

  override val broadcastNumBlocks: Int =
    broadcastVectorBytes.asInstanceOf[TorrentBroadcast[Dict]].numBlocks

  override lazy val valueCount: Int = dataFrame.count().toInt
}
