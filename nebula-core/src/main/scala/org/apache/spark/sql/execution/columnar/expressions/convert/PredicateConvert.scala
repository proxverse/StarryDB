package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions.{Expression, In, InSet, Literal}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConvert, NativeExpression}
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.sql.types.{ArrayType, BooleanType}

object PredicateConvert {}

object InConvert extends ExpressionConvertTrait {

  override def beforeConvert(expression: Expression): Expression = {
    val in = expression.asInstanceOf[In]
    if (in.list.isEmpty) {
      in.value match {
        case literal: Literal if literal.value == null =>
          Literal.create(null, BooleanType)
        case _ =>
          Literal.FalseLiteral
      }
    } else {
      in
    }
  }

  override def convert(functionName: String, expression: Expression): Expression = {
    val in = expression.asInstanceOf[In]
    if (in.value.isInstanceOf[ArrayType]) {
      throw new UnsupportedOperationException("Unsupport in set with ArrayType")
    }
    val values =
      if (in.list
            .map(_.asInstanceOf[NativeExpression])
            .forall(_.transformed.isInstanceOf[Literal])) {
        in.list
          .map(
            _.asInstanceOf[NativeExpression].transformed.asInstanceOf[expressions.Literal].value)
          .toArray
      } else {
        throw new UnsupportedOperationException("Unsupport in with other column")
      }

    val setExpression = ExpressionConvert.convertToNative(
      Literal
        .create(ArrayData.toArrayData(values), ArrayType.apply(in.value.dataType)))
    NativeExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExprHanlde(
          functionName,
          expression.dataType.catalogString,
          in.value.map(_.asInstanceOf[NativeExpression].handle).toArray ++ Array(
            setExpression
              .asInstanceOf[NativeExpression]
              .handle)),
      in)
  }

}

object InSetConvert extends ExpressionConvertTrait {

  override def beforeConvert(expression: Expression): Expression = {
    val inset = expression.asInstanceOf[InSet]
    if (inset.hset.isEmpty) {
      inset.child match {
        case literal: Literal if literal.value == null =>
          Literal.create(null, BooleanType)
        case _ =>
          Literal.FalseLiteral
      }
    } else {
      inset
    }
  }

  override def convert(functionName: String, expression: Expression): Expression = {
    val set = expression.asInstanceOf[InSet]

    if (set.child.isInstanceOf[ArrayType]) {
      throw new UnsupportedOperationException("Unsupport in set with ArrayType")
    }
    val setExpression = ExpressionConvert.convertToNative(
      Literal
        .create(ArrayData.toArrayData(set.hset.toArray), ArrayType.apply(set.child.dataType)))
    NativeExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExprHanlde(
          functionName,
          expression.dataType.catalogString,
          expression.children.map(_.asInstanceOf[NativeExpression].handle).toArray ++ Array(
            setExpression
              .asInstanceOf[NativeExpression]
              .handle)),
      expression)
  }

  override def lookupFunctionName(expression: Expression): Option[String] =
    Option.apply("in")

}
