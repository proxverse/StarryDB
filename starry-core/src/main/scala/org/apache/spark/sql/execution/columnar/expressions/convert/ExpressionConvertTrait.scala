package org.apache.spark.sql.execution.columnar.expressions.convert

import com.clearspring.analytics.util.Preconditions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, ExpressionNamingProcess, NativeJsonExpression}
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

  protected final def convertToNativeCall(
                         funcName: String, retType: DataType,
                         args: Seq[Expression], call: Expression,
                         skipResolve: Boolean = false): Expression = {
    Preconditions.checkArgument(args.forall(_.isInstanceOf[NativeJsonExpression]))
    ExpressionConverter.nativeCall(
      funcName,
      retType,
      args.map(_.asInstanceOf[NativeJsonExpression].native).toArray,
      call,
      skipResolve
    )
  }

}

abstract class AbstractExpressionConvertTrait[T <: Expression] extends ExpressionConvertTrait {
  def transformChildren(expression: T,
                        children: Seq[NativeJsonExpression]): Seq[NativeJsonExpression]

  override def convert(functionName: String, expression: Expression): Expression = {
    convertToNativeCall(
      functionName,
      expression.dataType,
      transformChildren(
        expression.asInstanceOf[T],
        expression.children.map(_.asInstanceOf[NativeJsonExpression])),
      expression)
  }
}

trait AggregateExpressionConvertTrait {

  def convert(expression: Expression, nativePlanBuilder: NativePlanBuilder): String

}
