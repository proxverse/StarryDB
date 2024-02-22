package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionNamingProcess, NativeExpression}
import org.apache.spark.sql.execution.columnar.jni.{NativeExpressionConvert, NativePlanBuilder}

trait ExpressionConvertTrait {

  def beforeConvert(expression: Expression): Expression = expression

  def convert(functionName: String, expression: Expression): Expression = {
    NativeExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExprHanlde(
          functionName,
          expression.dataType.catalogString,
          expression.children.map(_.asInstanceOf[NativeExpression].handle).toArray),
      expression)
  }

  def lookupFunctionName(expression: Expression): Option[String] = {
    ExpressionNamingProcess.defaultLookupFunctionName(expression)
  }

}

trait AggregateExpressionConvertTrait {

  def convert(expression: Expression, nativePlanBuilder: NativePlanBuilder): String

}
