package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.{
  DateFormatClass,
  Expression,
  Literal,
  TimestampDiff
}
import org.apache.spark.sql.execution.columnar.expressions.{
  ExpressionConverter,
  NativeJsonExpression
}

object TimestampDiffConverter extends AbstractExpressionConvertTrait[TimestampDiff] {
  override def transformChildren(
      expression: TimestampDiff,
      children: Seq[NativeJsonExpression]): Seq[NativeJsonExpression] = {
    val unit = ExpressionConverter.nativeConstant(Literal(expression.unit))
    Seq(unit.asInstanceOf[NativeJsonExpression]) ++ children
  }
}

object DateFormatConverter extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = {
    expression match {
      case dateFormat: DateFormatClass =>
        dateFormat.right match {
          case literal: Literal =>
            if (literal.toString().equals("yyyy-MM")) {
              dateFormat.copy(right = Literal("%Y-%m"))
            } else if (literal.toString().equals("yyyy-MM-dd")) {
              dateFormat.copy(right = Literal("%Y-%m-%d"))
            } else {
              throw new UnsupportedOperationException(
                s"date_format unsupported ${dateFormat.right.toString()}")
            }
          case other =>
            throw new UnsupportedOperationException(
              s"date_format unsupported ${dateFormat.right.toString()}")
        }
      case other => other
    }
  }
}
