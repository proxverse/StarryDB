package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

trait WrappedColumnarVector extends ColumnVector {

  def getWrapped: ColumnVector

}

case class NoCloseColumnVector(wrapped: ColumnVector)
  extends ColumnVector(wrapped.dataType)
  with WrappedColumnarVector {

  override def getWrapped: ColumnVector = wrapped

  private var refCount = 1

  /**
   * Don't actually close the ColumnVector this wraps. The producer of the vector will take care of
   * that.
   */
  override def close(): Unit = {
    // Empty
  }

  override def hasNull: Boolean = wrapped.hasNull

  override def numNulls(): Int = wrapped.numNulls

  override def isNullAt(rowId: Int): Boolean = wrapped.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = wrapped.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = wrapped.getByte(rowId)

  override def getShort(rowId: Int): Short = wrapped.getShort(rowId)

  override def getInt(rowId: Int): Int = wrapped.getInt(rowId)

  override def getLong(rowId: Int): Long = wrapped.getLong(rowId)

  override def getFloat(rowId: Int): Float = wrapped.getFloat(rowId)

  override def getDouble(rowId: Int): Double = wrapped.getDouble(rowId)

  override def getArray(rowId: Int): ColumnarArray = wrapped.getArray(rowId)

  override def getMap(ordinal: Int): ColumnarMap = wrapped.getMap(ordinal)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    wrapped.getDecimal(rowId, precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = wrapped.getUTF8String(rowId)

  override def getBinary(rowId: Int): Array[Byte] = wrapped.getBinary(rowId)

  override def getChild(ordinal: Int): ColumnVector = wrapped.getChild(ordinal)
}
