package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, NativeJsonExpression}
import org.apache.spark.sql.types.StringType

object LikeConvert extends ExpressionConvertTrait {
  override def convert(nativeFunctionName: String, expression: Expression): Expression = {
    convertToNativeCall(
      nativeFunctionName,
      expression.dataType,
      expression.children ++ Array(
        ExpressionConvert.nativeConstant(Literal.create(expression.asInstanceOf[Like].escapeChar))),
      expression
    )
  }
}
object SplitConvert extends ExpressionConvertTrait {

//  override def lookupFunctionName(expression: Expression): Option[String] = expression match {
//    case stringSplit: StringSplit =>
//      stringSplit.regex match {
//        case literal: Literal if StringUtils.isRegex(literal.value.toString) =>
//          Option.apply("regex_split")
//        case other =>
//          Option.apply("split")
//      }
//    case other =>
//      Option.empty
//  }

  override def convert(nativeFunctionName: String, expression: Expression): Expression = {
    val stringSplit = expression.asInstanceOf[StringSplit]
    val (functionName, childrenHandle) =
      stringSplit.regex.asInstanceOf[NativeJsonExpression].original match {
        case literal: Literal if StringUtils.isRegex(literal.value.toString) =>
          if (literal.value.toString.length > 1) {
            return stringSplit
          }
          (
            "regex_split",
            stringSplit.children
              .dropRight(1)
              .map(_.asInstanceOf[NativeJsonExpression])
              .toArray)
        case other =>
          if (stringSplit.limit
                .asInstanceOf[NativeJsonExpression]
                .original
                .asInstanceOf[Literal]
                .value
                .asInstanceOf[Int] < 1) {
            (
              nativeFunctionName,
              stringSplit.children
                .dropRight(1)
                .map(_.asInstanceOf[NativeJsonExpression])
                .toArray)

          } else {
            (
              nativeFunctionName,
              stringSplit.children.map(_.asInstanceOf[NativeJsonExpression]).toArray)
          }
      }
    convertToNativeCall(
          functionName,
          stringSplit.dataType,
          childrenHandle,
          stringSplit)
  }

}

object ConcatConvert extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = {
    val concat = expression.asInstanceOf[Concat]
    if (concat.children.isEmpty) {
      Literal.create("", StringType)
    } else if (concat.children.size == 1) {
      concat.children.head
    } else {
      concat
    }
  }
}
