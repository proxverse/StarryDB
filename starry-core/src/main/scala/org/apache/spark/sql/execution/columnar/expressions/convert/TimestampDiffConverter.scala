package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.{
  DateFormatClass,
  Expression,
  Literal,
  TimestampDiff
}
import org.apache.spark.sql.execution.columnar.expressions.{
  DateDiff,
  ExpressionConverter,
  ExpressionNamingProcess,
  NativeJsonExpression
}
import org.apache.spark.sql.internal.StarryConf

object TimestampDiffConverter extends AbstractExpressionConvertTrait[TimestampDiff] {
  override def lookupFunctionName(expression: Expression): Option[String] = {
    if (StarryConf.newDateDiffEnabled) {
      expression.asInstanceOf[TimestampDiff].unit.toUpperCase() match {
        case "MILLISECOND" => Option.apply("starry_timestamp_diff")
        case "SECOND" => Option.apply("starry_timestamp_diff")
        case "MINUTE" => Option.apply("starry_timestamp_diff")
        case "HOUR" => Option.apply("starry_timestamp_diff")
        case _ => ExpressionNamingProcess.defaultLookupFunctionName(expression)
      }
    } else {
      ExpressionNamingProcess.defaultLookupFunctionName(expression)
    }
  }
  override def transformChildren(
      expression: TimestampDiff,
      children: Seq[NativeJsonExpression]): Seq[NativeJsonExpression] = {
    val unit = ExpressionConverter.nativeConstant(Literal(expression.unit))
    Seq(unit.asInstanceOf[NativeJsonExpression]) ++ children
  }
}

object DateDiffConverter extends AbstractExpressionConvertTrait[DateDiff] {
  override def lookupFunctionName(expression: Expression): Option[String] = {
    if (StarryConf.newDateDiffEnabled) {
      expression.asInstanceOf[DateDiff].unit.toUpperCase() match {
        case "MILLISECOND" => Option.apply("starry_date_diff")
        case "SECOND" => Option.apply("starry_date_diff")
        case "MINUTE" => Option.apply("starry_date_diff")
        case "HOUR" => Option.apply("starry_date_diff")
        case _ => ExpressionNamingProcess.defaultLookupFunctionName(expression)
      }
    } else {
      ExpressionNamingProcess.defaultLookupFunctionName(expression)
    }
  }
  override def transformChildren(
      expression: DateDiff,
      children: Seq[NativeJsonExpression]): Seq[NativeJsonExpression] = {
    val unit = ExpressionConverter.nativeConstant(Literal(expression.unit))
    Seq(unit.asInstanceOf[NativeJsonExpression]) ++ children
  }
}

object DateFormatConverter extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = {
    expression match {
      case dateFormat: DateFormatClass =>
        dateFormat.right match {
          case n: NativeJsonExpression =>
            n
          case literal: Literal =>
            if (literal.toString().equals("yyyy-MM")) {
              dateFormat.copy(right = Literal("%Y-%m"))
            } else if (literal.toString().equals("yyyy-MM-dd")) {
              dateFormat.copy(right = Literal("%Y-%m-%d"))
            } else {
              throw new UnsupportedOperationException(
                s"date_format unsupported ${dateFormat.right.toString()}")
            }
          case other =>
            throw new UnsupportedOperationException(
              s"date_format unsupported ${dateFormat.right.toString()}")
        }
      case other => other
    }
  }
}
