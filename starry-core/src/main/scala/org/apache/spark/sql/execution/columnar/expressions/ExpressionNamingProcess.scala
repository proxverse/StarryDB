package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.columnar.expressions.convert.{
  ExpressionConvertMapping,
  PQLExpressionMappings
}

object ExpressionNamingProcess {

  def lookupFunctionName(expression: Expression): Option[String] = {
    expression match {
      case o if ExpressionConvertMapping.expressionsMap.contains(o.getClass) =>
        ExpressionConvertMapping.expressionsMap.apply(o.getClass).lookupFunctionName(o)
      case other =>
        defaultLookupFunctionName(expression)
    }
  }

  def defaultLookupFunctionName(expression: Expression): Option[String] = {
    PQLExpressionMappings.expressionsMap
      .get(expression.getClass)
      .orElse(ExpressionMappings.expressionsMap.get(expression.getClass))
  }
}
