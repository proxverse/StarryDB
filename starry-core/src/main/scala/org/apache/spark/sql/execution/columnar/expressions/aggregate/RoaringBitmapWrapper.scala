package org.apache.spark.sql.execution.columnar.expressions.aggregate

import org.roaringbitmap.longlong.Roaring64Bitmap
import org.roaringbitmap.{RoaringBitmap, RoaringBitmapWriter}

import java.nio.ByteBuffer

case class RoaringBitmapWrapper(int64: Boolean) {

  lazy private val bitmapWriter_ = RoaringBitmapWriter
      .writer()
      .initialCapacity(4096) // Let's say I have historical data about this and want to reduce some allocations
      .optimiseForRuns() // in case you *know* the bitmaps typically built are very dense
      .get()
  lazy private val bitmap64_ = new Roaring64Bitmap

  var providedBitmapWriter: RoaringBitmapWriter[RoaringBitmap] = _
  var providedbitmap64: Roaring64Bitmap = _

  private def bitmapWriter = Option(providedBitmapWriter).getOrElse(bitmapWriter_)
  private def bitmap64 = Option(providedbitmap64).getOrElse(bitmap64_)

  def addValue(value: Any): Unit = {
    value match {
      case intValue: Int => bitmapWriter.add(intValue)
      case longValue: Long => bitmap64.addLong(longValue)
      case _ => throw new UnsupportedOperationException(s"unsupported type bitmap value $value")
    }
  }

  def or(other: RoaringBitmapWrapper): Unit = {
    if (int64) {
      bitmap64.or(other.bitmap64)
    } else {
      bitmapWriter.get().or(other.bitmapWriter.get())
    }
  }

  def and(other: RoaringBitmapWrapper): Unit = {
    if (int64) {
      bitmap64.and(other.bitmap64)
    } else {
      bitmapWriter.get().and(other.bitmapWriter.get())
    }
  }

  def getLongCardinality(): Long = {
    if (int64) {
      bitmap64.getLongCardinality
    } else {
      bitmapWriter.get().getLongCardinality
    }
  }

  def serialize(): Array[Byte] = {
    if (int64) {
      bitmap64.runOptimize()
      if (bitmap64.serializedSizeInBytes() + 1 >= Integer.MAX_VALUE) {
        throw new IllegalStateException(
          s"roaring bitmap64's buffer size is too large: size = (${bitmap64.serializedSizeInBytes()})")
      }
      val buf = ByteBuffer.allocate(bitmap64.serializedSizeInBytes().toInt + 1)
      buf.put(1.toByte)
      bitmap64.serialize(buf)
      buf.array()
    } else {
      val bitmap = bitmapWriter.get()
      val buf = ByteBuffer.allocate(bitmap.serializedSizeInBytes() + 1)
      buf.put(0.toByte)
      bitmap.serialize(buf)
      buf.array()
    }
  }

  def read(buf: ByteBuffer): Unit = {
    if (int64) {
      bitmap64.deserialize(buf)
    } else {
      val bitmap = bitmapWriter.get()
      bitmap.deserialize(buf)
    }
  }

}

object RoaringBitmapWrapper {

  def deserialize(bytes: Array[Byte]): RoaringBitmapWrapper = {
    val buf = ByteBuffer.wrap(bytes)
    val int64 = buf.get() == 1
    val wrapper = new RoaringBitmapWrapper(int64)
    wrapper.read(buf)
    wrapper
  }

}
