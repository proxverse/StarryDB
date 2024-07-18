package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Literal, UnaryExpression}
import org.apache.spark.sql.execution.columnar.expressions.aggregate.RoaringBitmapWrapper
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.util.Utils

case class BitmapContains(child: Expression, bitmap: Expression) extends BinaryExpression {

  private def bitmapByteSize = Utils.bytesToString(
    bitmap.asInstanceOf[Literal].value.asInstanceOf[Array[Byte]].length)

  override def left: Expression = child

  override def right: Expression = bitmap

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, bitmap = newRight)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new UnsupportedOperationException()
  }

  override def dataType: DataType = BooleanType

  override def toString(): String = s"bitmap_contains($child, bitmap(${bitmapByteSize}))"

}
