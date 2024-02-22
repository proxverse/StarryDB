package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.expressions.{Concat, CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, NativeExpression}
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert

object RowConvert {}

object CreateNamedStructConvert extends ExpressionConvertTrait {
  override def convert(functionName: String, expression: Expression): Expression = {
    val struct = expression.asInstanceOf[CreateNamedStruct]
    try {
      val after = NativeExpression(
        NativeExpressionConvert
          .nativeCreateCallTypedExprHanlde(
            functionName,
            expression.dataType.catalogString,
            struct.valExprs.map(_.asInstanceOf[NativeExpression].handle).toArray),
        expression.withNewChildren(
          expression.children.map(_.asInstanceOf[NativeExpression]).map(_.transformed)))

      struct.nameExprs.foreach(ExpressionConvert.releaseHandle)
      after
    } catch {
      case e =>
        expression
    }

  }

}
