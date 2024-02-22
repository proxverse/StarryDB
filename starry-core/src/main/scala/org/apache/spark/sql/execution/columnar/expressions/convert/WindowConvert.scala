package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.expressions.{
  ExpressionNamingProcess,
  NativeExpression
}
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.sql.types.{LongType, NullType}

object WindowConvert extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = expression match {
    case lead: Lead =>
      val value = lead.second.eval().toString.toLong
      if (value < 0) {
        Lag(lead.first, Literal(value.abs, LongType), lead.third, lead.ignoreNulls)
      } else {
        lead.withNewChildren(
          Seq(lead.first, Literal(lead.second.eval().toString.toLong, LongType), lead.third))
      }
    case lag: Lag =>
      val value = lag.second.eval().toString.toLong
      if (value < 0) {
        Lead(lag.first, Literal(value.abs, LongType), lag.third, lag.ignoreNulls)
      } else {
        lag.withNewChildren(
          Seq(lag.first, Literal(lag.second.eval().toString.toLong, LongType), lag.third))
      }
    case nth: NthValue =>
      nth.withNewChildren(Seq(nth.input, Literal(nth.offset.eval().toString.toLong, LongType)))
    case other => other
  }

  override def convert(functionName: String, expression: Expression): Expression = {
    val children = expression match {
      case lead: Lead =>
        if (lead.third.dataType.sameType(NullType)) {
          lead.children.dropRight(1)
        } else {
          lead.children
        }

      case lag: Lag =>
        if (lag.third.dataType.sameType(NullType)) {
          lag.children.dropRight(1)
        } else {
          lag.children
        }
      case wf @ (RowNumber() | Rank(_) | DenseRank(_) | CumeDist() | PercentRank(_)) =>
        Seq.empty
      case ohter => expression.children
    }
    NativeExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExprHanlde(
          functionName,
          expression.dataType.catalogString,
          children.map(_.asInstanceOf[NativeExpression].handle).toArray),
      expression)
  }

  override def lookupFunctionName(expression: Expression): Option[String] = expression match {
    case lag: Lag =>
      Option.apply("lag")
    case lead: Lead =>
      Option.apply("lead")
    case nthValue: NthValue =>
      Option.apply("nth_value")
    case other => ExpressionNamingProcess.defaultLookupFunctionName(other)

  }

}
