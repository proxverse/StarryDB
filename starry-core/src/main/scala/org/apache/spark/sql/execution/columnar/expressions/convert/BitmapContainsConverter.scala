package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.columnar.expressions.{BitmapContains, ExpressionConverter, NativeJsonExpression}

object BitmapContainsConverter extends ExpressionConvertTrait {
  override def convert(functionName: String, expression: Expression): Expression = {
    val children = expression match {
      case BitmapContains(child, bitmap) =>
        Array(ExpressionConverter.convertToNative(child),
          ExpressionConverter.convertToNative(bitmap))
    }
    convertToNativeCall(
      "starry_bitmap_contains",
      expression.dataType,
      children.map(_.asInstanceOf[NativeJsonExpression]),
      expression
    )
  }

  override def lookupFunctionName(expression: Expression): Option[String] = Some("starry_bitmap_contains")
}
