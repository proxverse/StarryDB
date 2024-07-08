package org.apache.spark.sql.execution.columnar.expressions.convert

import com.google.common.base.Preconditions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.expressions.{
  ExpressionConverter,
  NativeJsonExpression
}
import org.apache.spark.sql.execution.columnar.extension.rule.NativeFunctionPlaceHolder
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}

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

  override def convert(functionName: String, expression: Expression): Expression = {
    val array = expression
      .asInstanceOf[SortArray]
    val ascendingOrder = array.ascendingOrder
      .asInstanceOf[NativeJsonExpression]
      .original
      .asInstanceOf[Literal]
      .value
      .toString
      .toBoolean
    val functionName = if (ascendingOrder) {
      "array_sort"
    } else {
      "array_sort_desc"
    }
    val holder = NativeFunctionPlaceHolder(array, array.base :: Nil, array.dataType, functionName)
    convertToNativeCall(functionName, array.dataType, array.base :: Nil, holder)
  }
}

object SizeConvert extends ExpressionConvertTrait {
  override def convert(functionName: String, expression: Expression): Expression = {
    val size = expression.asInstanceOf[Size]
    convertToNativeCall(
      functionName,
      expression.dataType,
      expression.children ++ Seq(
        ExpressionConverter.nativeConstant(Literal.create(size.legacySizeOfNull, BooleanType))),
      expression)
  }
}
