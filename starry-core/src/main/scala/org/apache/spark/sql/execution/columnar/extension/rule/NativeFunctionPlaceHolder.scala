package org.apache.spark.sql.execution.columnar.extension.rule

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType}
import org.sparkproject.jetty.server.Response.OutputType

case class NativeFunctionPlaceHolder(original: AggregateFunction,
                                     inputs: Seq[Expression],
                                     outputType: DataType,
                                     functionName: String)
  extends AbstractNativeFunctionPlaceHolder {

  def this(original: AggregateFunction, inputs: Seq[Expression], outputType: DataType) = {
    this(original, inputs, outputType, original.prettyName)
  }

  def this(original: AggregateFunction, inputs: Seq[Expression]) = {
    this(original, inputs, original.dataType, original.prettyName)
  }

  def this(original: AggregateFunction, outputType: DataType) = {
    this(original, original.children, outputType, original.prettyName)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(original, newChildren, outputType)

  override def prettyName: String = functionName

  override def toString(): String = s"native_holder($original)"

}

abstract class AbstractNativeFunctionPlaceHolder
  extends DeclarativeAggregate with ImplicitCastInputTypes {

  def original: AggregateFunction
  def inputs: Seq[Expression]
  def outputType: DataType

  override lazy val initialValues: Seq[Expression] = throw new UnsupportedOperationException

  override lazy val updateExpressions: Seq[Expression] = throw new UnsupportedOperationException

  override lazy val mergeExpressions: Seq[Expression] = throw new UnsupportedOperationException

  override lazy val evaluateExpression: Expression = throw new UnsupportedOperationException

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    AttributeReference("dummy", IntegerType)() :: Nil

  override def inputTypes: Seq[AbstractDataType] =
    inputs.filter(!_.isInstanceOf[Literal]).map(_.dataType)

  override def nullable: Boolean = original.nullable

  override def dataType: DataType = outputType

  override def children: Seq[Expression] = inputs

  override def prettyName: String = original.prettyName
}
