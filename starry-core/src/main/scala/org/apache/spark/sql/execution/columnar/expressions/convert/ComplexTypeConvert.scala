package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.expressions.NativeExpression
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.sql.types.IntegerType

object ComplexTypeConvert {}

object GetArrayItemConvert extends ExpressionConvertTrait {

  override def beforeConvert(expression: Expression): Expression = {
    val getArrayItem = expression.asInstanceOf[GetArrayItem]
    getArrayItem.ordinal match {
      case lit: Literal if lit.value != null =>
        GetArrayItem(
          getArrayItem.child,
          Literal.create(lit.value.toString.toInt + 1, IntegerType))
      case other =>
        GetArrayItem(
          getArrayItem.child,
          Add(getArrayItem.ordinal, Literal.create(1, IntegerType)))
    }
  }

  override def lookupFunctionName(expression: Expression): Option[String] =
    Option.apply("element_at")
}

object SortArrayConvert extends ExpressionConvertTrait {
  override def lookupFunctionName(expression: Expression): Option[String] = {
    val ascendingOrder = expression
      .asInstanceOf[SortArray]
      .ascendingOrder
      .asInstanceOf[NativeExpression]
      .transformed
      .asInstanceOf[Literal]
      .value
      .toString
      .toBoolean
    if (ascendingOrder) {
      Option.apply("array_sort")
    } else {
      Option.apply("array_sort_desc")
    }
  }

  override def convert(functionName: String, expression: Expression): Expression = {
    NativeExpression(
      NativeExpressionConvert
          .nativeCreateCallTypedExprHanlde(
            functionName,
            expression.dataType.catalogString,
            expression.children.dropRight(1).map(_.asInstanceOf[NativeExpression].handle).toArray),
      expression)
  }
}
