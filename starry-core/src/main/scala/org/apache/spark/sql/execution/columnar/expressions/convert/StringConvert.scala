package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.expressions.ExpressionConvert.convertToNative
import org.apache.spark.sql.execution.columnar.expressions.NativeExpression
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.sql.types.StringType

object LikeConvert extends ExpressionConvertTrait {
  override def convert(nativeFunctionName: String, expression: Expression): Expression = {
    NativeExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExprHanlde(
          nativeFunctionName,
          expression.dataType.catalogString,
          expression.children.map(_.asInstanceOf[NativeExpression].handle).toArray ++ Array(
            convertToNative(Literal.create(expression.asInstanceOf[Like].escapeChar))
              .asInstanceOf[NativeExpression]
              .handle)),
      expression)
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
      stringSplit.regex.asInstanceOf[NativeExpression].transformed match {
        case literal: Literal if StringUtils.isRegex(literal.value.toString) =>
          if (literal.value.toString.length > 1) {
            return stringSplit
          }
          (
            "regex_split",
            stringSplit.children
              .dropRight(1)
              .map(_.asInstanceOf[NativeExpression].handle)
              .toArray)
        case other =>
          if (stringSplit.limit
                .asInstanceOf[NativeExpression]
                .transformed
                .asInstanceOf[Literal]
                .value
                .asInstanceOf[Int] < 1) {
            (
              nativeFunctionName,
              stringSplit.children
                .dropRight(1)
                .map(_.asInstanceOf[NativeExpression].handle)
                .toArray)

          } else {
            (
              nativeFunctionName,
              stringSplit.children.map(_.asInstanceOf[NativeExpression].handle).toArray)
          }
      }
    NativeExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExprHanlde(
          functionName,
          stringSplit.dataType.catalogString,
          childrenHandle),
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
