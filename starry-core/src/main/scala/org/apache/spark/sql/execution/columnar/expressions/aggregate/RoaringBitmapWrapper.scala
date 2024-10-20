package org.apache.spark.sql.execution.columnar.expressions.aggregate

import org.apache.commons.codec.binary.Hex
import org.roaringbitmap.longlong.Roaring64Bitmap
import org.roaringbitmap.{RoaringBitmap, RoaringBitmapWriter}

import java.nio.ByteBuffer

case class RoaringBitmapWrapper(int64: Boolean) {

  lazy private val bitmapWriter_ = RoaringBitmapWriter
      .writer()
      .initialCapacity(4096) // Let's say I have historical data about this and want to reduce some allocations
      .optimiseForRuns() // in case you *know* the bitmaps typically built are very dense
      .get()
  private var bitmap_ : RoaringBitmap = _
  lazy private val bitmap64_ = new Roaring64Bitmap

  var providedBitmapWriter: RoaringBitmapWriter[RoaringBitmap] = _
  var providedbitmap64: Roaring64Bitmap = _

  private def bitmapWriter = Option(providedBitmapWriter).getOrElse(bitmapWriter_)
  private def bitmap64 = Option(providedbitmap64).getOrElse(bitmap64_)

  // FLUSH before calling contains if any updates
  def flush(): Unit = {
    if (!int64) {
      bitmap_ = bitmapWriter_.get()
    }
  }

  // FLUSH before calling contains if any updates
  def contains(value: Any): Boolean = {
    value match {
      case intValue: Int if !int64 => bitmap_.contains(intValue)
      case longValue: Long if int64 => bitmap64.contains(longValue)
      case _ => throw new UnsupportedOperationException(
        s"Unsupported type bitmap value $value, " +
        s"expected ${if (int64) "long" else "int" } type")
    }
  }

  def addValue(value: Any): Unit = {
    value match {
      case intValue: Int => bitmapWriter.add(intValue)
      case longValue: Long => bitmap64.addLong(longValue)
      case _ => throw new UnsupportedOperationException(s"Unsupported type bitmap value $value")
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

  def serialize(withTypeFlag: Boolean = true): Array[Byte] = {
    if (int64) {
      bitmap64.runOptimize()
      if (bitmap64.serializedSizeInBytes() + 1 >= Integer.MAX_VALUE) {
        throw new IllegalStateException(
          s"roaring bitmap64's buffer size is too large: size = (${bitmap64.serializedSizeInBytes()})")
      }
      val buf = if (withTypeFlag) {
        val withFlag = ByteBuffer.allocate(bitmap64.serializedSizeInBytes().toInt + 1)
        withFlag.put(1.toByte)
        withFlag
      } else {
        ByteBuffer.allocate(bitmap64.serializedSizeInBytes().toInt)
      }
      bitmap64.serialize(buf)
      buf.array()
    } else {
      val bitmap = bitmapWriter.get()
      val buf = if (withTypeFlag) {
        val withFlag = ByteBuffer.allocate(bitmap.serializedSizeInBytes() + 1)
        withFlag.put(0.toByte)
        withFlag
      } else {
        ByteBuffer.allocate(bitmap.serializedSizeInBytes())
      }
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
    wrapper.flush()
    wrapper
  }

  def deserialize(bytes: Array[Byte], int64: Boolean): RoaringBitmapWrapper = {
    val buf = ByteBuffer.wrap(bytes)
    val wrapper = new RoaringBitmapWrapper(int64)
    wrapper.read(buf)
    wrapper.flush()
    wrapper
  }


}
