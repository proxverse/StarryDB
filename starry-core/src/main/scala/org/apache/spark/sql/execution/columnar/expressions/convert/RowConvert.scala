package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, GetArrayStructFields, Literal}
import org.apache.spark.sql.execution.columnar.expressions.NativeJsonExpression
import org.apache.spark.sql.execution.columnar.extension.rule.NativeScalarFunctionPlaceHolder
import org.apache.spark.sql.types.IntegerType

object RowConvert {}

object CreateNamedStructConvert extends ExpressionConvertTrait {
  override def convert(functionName: String, expression: Expression): Expression = {
    val struct = expression.asInstanceOf[CreateNamedStruct]
    val origin = expression.withNewChildren(
      expression.children.map(_.asInstanceOf[NativeJsonExpression]).map(_.original))
    convertToNativeCall(functionName, origin.dataType, struct.valExprs, origin)
  }

}

object GetArrayStructFieldsConverter extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = {
    val struct = expression.asInstanceOf[GetArrayStructFields]
    NativeScalarFunctionPlaceHolder(
      struct,
      struct.child
        :: Literal(struct.ordinal, IntegerType)
        :: Nil,
      struct.dataType,
      "get_array_struct_fields")
  }
}


