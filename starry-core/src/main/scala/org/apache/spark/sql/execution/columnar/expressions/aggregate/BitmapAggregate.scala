/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.columnar.expressions.aggregate

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  ImperativeAggregate,
  TypedImperativeAggregate
}
import org.apache.spark.sql.types._
import org.roaringbitmap.{RoaringBitmap, RoaringBitmapWriter}

import java.nio.ByteBuffer

// scalastyle:off
@ExpressionDescription(usage = "PreciseCountDistinct(expr)")
@SerialVersionUID(1)
sealed abstract class BasicBitmapFunction(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[RoaringBitmap]
    with Serializable
    with Logging {

  var time = 0L

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  var writer: RoaringBitmapWriter[RoaringBitmap] = null
  override def createAggregationBuffer(): RoaringBitmap = {
    writer = RoaringBitmapWriter
      .writer()
      .initialCapacity(4096) // Let's say I have historical data about this and want to reduce some allocations
      .optimiseForRuns() // in case you *know* the bitmaps typically built are very dense
      .get()
    new RoaringBitmap()
  }

  override def merge(buffer: RoaringBitmap, input: RoaringBitmap): RoaringBitmap = {
    buffer.or(input)
    buffer
  }

  var array: Array[Byte] = _

  override def serialize(buffer: RoaringBitmap): Array[Byte] = {
    try {
      val bitmap = writer.get()
      val buf = ByteBuffer.allocate(bitmap.serializedSizeInBytes())
      bitmap.serialize(buf)
      buf.array()
    } catch {
      case th =>
        throw th
    }
  }

  override def deserialize(bytes: Array[Byte]): RoaringBitmap = {
    val bitMap = new RoaringBitmap()
    if (bytes.nonEmpty) {
      bitMap.deserialize(ByteBuffer.wrap(bytes))
    }
    bitMap
  }

}

@SerialVersionUID(1)
case class ReusePreciseCountDistinct(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends BasicBitmapFunction(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = BinaryType

  override def update(buffer: RoaringBitmap, input: InternalRow): RoaringBitmap = {
    val colValue = child.eval(input)
    buffer.or(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: RoaringBitmap): Any = {
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(newChildren.head)
}

@SerialVersionUID(1)
case class BitmapAndAggFunction(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends BasicBitmapFunction(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = LongType

  override def update(buffer: RoaringBitmap, input: InternalRow): RoaringBitmap = {
    val colValue = child.eval(input)
    buffer.and(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: RoaringBitmap): Any = {
    buffer.getLongCardinality
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(newChildren.head)

}

@SerialVersionUID(1)
case class BitmapOrAggFunction(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends BasicBitmapFunction(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = LongType

  override def update(buffer: RoaringBitmap, input: InternalRow): RoaringBitmap = {
    val colValue = child.eval(input)
    buffer.and(deserialize(colValue.asInstanceOf[Array[Byte]]))
    buffer
  }

  override def eval(buffer: RoaringBitmap): Any = {
    buffer.getLongCardinality
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(newChildren.head)

}

@SerialVersionUID(1)
case class BitmapConstructAggFunction(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends BasicBitmapFunction(child, mutableAggBufferOffset, inputAggBufferOffset) {

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = BinaryType

  override def update(buffer: RoaringBitmap, input: InternalRow): RoaringBitmap = {
    val colValue = child.eval(input)
    if (colValue != null) {
      buffer.add(colValue.asInstanceOf[Int])
    }
    buffer
  }

  override def eval(buffer: RoaringBitmap): Any = {
    serialize(buffer)
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(newChildren.head)

}

case class BitmapCountDistinctAggFunction(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends BasicBitmapFunction(child, mutableAggBufferOffset, inputAggBufferOffset) {

  override val prettyName: String = "bitmap_count_distinct"

  def this(child: Expression) = this(child, 0, 0)

  override def dataType: DataType = LongType

  override def update(buffer: RoaringBitmap, input: InternalRow): RoaringBitmap = {
    val colValue = child.eval(input)
    if (colValue != null) {
      writer.add(colValue.asInstanceOf[Int])
    }
    buffer
  }

  override def eval(buffer: RoaringBitmap): Any = {
    buffer.getLongCardinality
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(newChildren.head)

}
