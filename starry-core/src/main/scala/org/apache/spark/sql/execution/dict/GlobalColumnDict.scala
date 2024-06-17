package org.apache.spark.sql.execution.dict

import org.apache.spark.broadcast.{Broadcast, TorrentBroadcast}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.StarryContext
import org.apache.spark.sql.execution.columnar.{ColumnBatchUtils, VeloxColumnarBatch}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String

trait ColumnDict {

  def supportExecution: Boolean = true

  def cleanup(): Unit

  def broadcastID: Long

  def broadcastNumBlocks: Int

  def dataType: DataType = StringType

  def valueCount: Int

  @transient lazy private val valuesToId: Map[UTF8String, Int] = {
    val rows = veloxColumnarBatch.numRows()
    Range(0, rows).map { index =>
      (veloxColumnarBatch.column(0).getUTF8String(index), index)
    }.toMap
  }

  lazy val veloxColumnarBatch: VeloxColumnarBatch = {
    ExecutorDictManager.dictCache.get((broadcastID, broadcastNumBlocks))
  }

  def vector(): ColumnVector = {
    veloxColumnarBatch.column(0)
  }

  def valueToDictId(value: UTF8String): Int = {
    if (valuesToId.contains(value)) {
      valuesToId.apply(value)
    } else {
      -1
    }
  }

  def valueToDictId(value: Array[Byte]): Int = {
    if (valuesToId.contains(UTF8String.fromBytes(value))) {
      valuesToId.apply(UTF8String.fromBytes(value))
    } else {
      -1
    }
  }

}

trait TempDict extends ColumnDict with Logging {

  if (StarryContext.get().isDefined) {
    StarryContext.get().get.addTempDict(this)
  } else {
    logWarning(s"Creating temp dict $this without StarryContext")
  }

}

case class SimpleColumnDict(@transient dictValues: Array[UTF8String])
    extends ColumnDict
    with Logging {

  override def toString: String = "column dict"

  def this(values: Array[String]) = {
    this(values.map(UTF8String.fromString))
  }

  override def cleanup(): Unit = {
    broadcastVectorBytes.destroy()
  }

  override val broadcastID: Long = broadcastVectorBytes.id

  override val broadcastNumBlocks: Int =
    broadcastVectorBytes.asInstanceOf[TorrentBroadcast[Dict]].numBlocks

  @transient lazy val broadcastVectorBytes: Broadcast[IDict] = {
    val schema = StructType(Seq(StructField("dict", StringType)))
    val batch = ColumnBatchUtils.createWriterableColumnBatch(dictValues.length, schema)
    try {
      val start = System.currentTimeMillis()
      Range(0, dictValues.length)
        .foreach { i =>
          batch
            .column(0)
            .asInstanceOf[WritableColumnVector]
            .putByteArray(i, dictValues.apply(i).getBytes)
        }
      batch.setNumRows(dictValues.length)
      log.info(s"Create dict broadcast take time ${System.currentTimeMillis() - start} ms")
      val rowVector = batch.asInstanceOf[VeloxColumnarBatch].rowVector()
      val bytes = rowVector.serialize()
      rowVector.close()
      SparkSession.active.sparkContext.broadcast(Dict(bytes))
    } finally {
      batch.close()
    }
  }

  override def valueCount: Int = dictValues.length
}

case class ExecutionColumnDict(
    @transient columnDict: ColumnDict,
    expression: Expression,
    dt: DataType,
    nativeExpression: String = null)
    extends TempDict
    with Logging {

  if (columnDict.isInstanceOf[StartEndDict]) {
    throw new UnsupportedOperationException("exec start end dict is not supported yet")
  }

  override def toString: String = s"dict execution: ${expression}, dict: $columnDict"

  override def dataType: DataType = dt

  override def cleanup(): Unit = {
    broadcastVectorBytes.destroy(false)
    logInfo(s"remove execution dict id ${broadcastVectorBytes.id}")
  }

  override val broadcastID: Long = broadcastVectorBytes.id

  override val broadcastNumBlocks: Int =
    broadcastVectorBytes.asInstanceOf[TorrentBroadcast[ExecutionDict]].numBlocks

  @transient lazy val broadcastVectorBytes: Broadcast[IDict] = {
    SparkSession.active.sparkContext
      .broadcast(
        ExecutionDict(
          columnDict.broadcastID,
          columnDict.broadcastNumBlocks,
          expression.dataType,
          nativeExpression))
  }

  override def valueCount: Int = columnDict.valueCount
}

// TODO StartEndDict should be in proxverse-project
case class StartEndDict(start: UTF8String, end: UTF8String, wrappedDict: ColumnDict)
    extends TempDict
    with Logging {

  override val broadcastID: Long = wrappedDict.broadcastID

  override val broadcastNumBlocks: Int = wrappedDict.broadcastNumBlocks

  override def cleanup(): Unit = { /* do nothing, wrappedDict should not be cleaned here */ }

  override def toString: String = s"StartEndDict($start,$end,$wrappedDict)"

  override def valueCount: Int = wrappedDict.valueCount

  private lazy val startEndVector: ColumnVector = {
    val rows = veloxColumnarBatch.numRows()
    val newVector = new OnHeapColumnVector(rows + 2, veloxColumnarBatch.column(0).dataType())
    Range(0, rows).foreach { index =>
      newVector.putByteArray(index, veloxColumnarBatch.column(0).getUTF8String(index).getBytes)
    }
    newVector.putByteArray(rows, start.getBytes)
    newVector.putByteArray(rows + 1, end.getBytes)
    newVector
  }

  override def vector(): ColumnVector = {
    startEndVector
  }

}
