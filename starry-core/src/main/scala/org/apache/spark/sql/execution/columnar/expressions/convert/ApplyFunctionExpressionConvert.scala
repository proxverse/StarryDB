package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.expressions.ExpressionNamingProcess
import org.apache.spark.sql.types.{LongType, StringType}

object ApplyFunctionExpressionConvert extends ExpressionConvertTrait {
  override def lookupFunctionName(expression: Expression): Option[String] = {
    Option.apply(expression.asInstanceOf[ApplyFunctionExpression].function.name())
  }
}
