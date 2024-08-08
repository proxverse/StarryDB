package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UnaryExpression}
import org.apache.spark.sql.execution.columnar.expressions.aggregate.RoaringBitmapWrapper
import org.apache.spark.sql.types.{BooleanType, DataType, LongType}
import org.apache.spark.util.Utils

case class BitmapContains(child: Expression, bitmap: Expression) extends UnaryExpression {

  private def bitmapByteSize = Utils.bytesToString(
    bitmap.asInstanceOf[Literal].value.asInstanceOf[Array[Byte]].length)


  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild, bitmap)


  @transient
  lazy val wrapper = RoaringBitmapWrapper.deserialize(
    bitmap.asInstanceOf[Literal].value.asInstanceOf[Array[Byte]],
    child.dataType == LongType)

  override protected def nullSafeEval(input: Any): Any = {
    wrapper.contains(input)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val bitmapBytes = ctx.addReferenceObj("bitmapBytes", bitmap.asInstanceOf[Literal].value.asInstanceOf[Array[Byte]])
    val isInt64 = if (child.dataType == LongType) "true" else "false"
    val bitmapWrapper = "bitmapWrapper"
    val wrapperClz = "org.apache.spark.sql.execution.columnar.expressions.aggregate.RoaringBitmapWrapper"
    ctx.addImmutableStateIfNotExists(
      wrapperClz,
      bitmapWrapper,
      v => s"$v = $wrapperClz.deserialize($bitmapBytes, $isInt64);"
    )
    defineCodeGen(ctx, ev, child => {
      s"""
         |${ev.value} = $bitmapWrapper.contains($child);
         |""".stripMargin
    })
  }

  override def dataType: DataType = BooleanType

  override def toString(): String = s"bitmap_contains($child, bitmap(${bitmapByteSize}))"

}

