package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression}
import org.apache.spark.sql.execution.columnar.expressions.NativeJsonExpression

object RowConvert {}

object CreateNamedStructConvert extends ExpressionConvertTrait {
  override def convert(functionName: String, expression: Expression): Expression = {
    val struct = expression.asInstanceOf[CreateNamedStruct]
    try {
      val after = convertToNativeCall(
        functionName,
        expression.dataType,
        struct.valExprs,
        expression.withNewChildren(
          expression.children.map(_.asInstanceOf[NativeJsonExpression]).map(_.original)))

      after
    } catch {
      case e =>
        expression
    }

  }

}
