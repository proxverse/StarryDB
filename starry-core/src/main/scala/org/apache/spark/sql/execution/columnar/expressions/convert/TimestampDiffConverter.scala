package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.{Literal, TimestampDiff}
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, NativeJsonExpression}

object TimestampDiffConverter extends AbstractExpressionConvertTrait[TimestampDiff] {
  override def transformChildren(expression: TimestampDiff,
                                 children: Seq[NativeJsonExpression]): Seq[NativeJsonExpression] = {
    val unit = ExpressionConverter.nativeConstant(Literal(expression.unit))
    Seq(unit.asInstanceOf[NativeJsonExpression]) ++ children
  }
}
