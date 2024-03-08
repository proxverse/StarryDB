package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Expression, In, InSet, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.columnar.expressions.{ExpressionConverter, NativeJsonExpression}
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
            .map(_.asInstanceOf[NativeJsonExpression])
            .forall(_.original.isInstanceOf[Literal])) {
        in.list
          .map(
            _.asInstanceOf[NativeJsonExpression].original.asInstanceOf[expressions.Literal].value)
          .toArray
      } else {
        throw new UnsupportedOperationException("Unsupport in with other column")
      }

    val setExpression = ExpressionConverter.nativeConstant(
      Literal
        .create(ArrayData.toArrayData(values), ArrayType.apply(in.value.dataType)))
    convertToNativeCall(
      functionName,
      expression.dataType,
      in.value :: setExpression :: Nil,
      in
    )
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
    val setExpression = ExpressionConverter.nativeConstant(
      Literal
        .create(ArrayData.toArrayData(set.hset.toArray), ArrayType.apply(set.child.dataType)))
    convertToNativeCall(
      functionName,
      expression.dataType,
      expression.children ++ Seq(setExpression),
      expression
    )
  }

  override def lookupFunctionName(expression: Expression): Option[String] =
    Option.apply("in")

}
