package org.apache.spark.sql.execution.columnar.expressions.convert

import com.clearspring.analytics.util.Preconditions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, ExpressionNamingProcess, NativeJsonExpression}
import org.apache.spark.sql.execution.columnar.jni.NativePlanBuilder
import org.apache.spark.sql.types.DataType

trait ExpressionConvertTrait {

  def beforeConvert(expression: Expression): Expression = expression

  def convert(functionName: String, expression: Expression): Expression = {
    convertToNativeCall(
      functionName,
      expression.dataType,
      expression.children,
      expression)
  }

  def lookupFunctionName(expression: Expression): Option[String] = {
    ExpressionNamingProcess.defaultLookupFunctionName(expression)
  }

  protected def convertToNativeCall(funcName: String, retType: DataType,
                          args: Seq[Expression], call: Expression): Expression = {
    Preconditions.checkArgument(args.forall(_.isInstanceOf[NativeJsonExpression]))
    ExpressionConvert.nativeCall(
      funcName,
      retType,
      args.map(_.asInstanceOf[NativeJsonExpression].native).toArray,
      call
    )
  }

}

trait AggregateExpressionConvertTrait {

  def convert(expression: Expression, nativePlanBuilder: NativePlanBuilder): String

}
