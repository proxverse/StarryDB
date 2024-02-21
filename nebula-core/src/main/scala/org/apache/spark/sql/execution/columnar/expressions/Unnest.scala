
package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

case class Unnest(children: Seq[Expression])
  extends Expression
  with CollectionGenerator
  with Serializable {
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev
  }

  override def position: Boolean = false

  override def inline: Boolean = true

  override def elementSchema: StructType = {
    new StructType(
      children.zipWithIndex
        .map(
          child =>
            child._1 match {
              case other =>
                StructField(
                  s"source_${child._2}",
                  child._1.dataType.asInstanceOf[ArrayType].elementType)
            })
        .toArray)
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    return Unnest(newChildren)
  }
}
