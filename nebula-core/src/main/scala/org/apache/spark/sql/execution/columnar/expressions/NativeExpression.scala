
package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

case class NativeExpression(handle: Long, transformed: Expression) extends ColumnarExpression {
  override def nullable: Boolean = transformed.nullable

  override def eval(input: InternalRow): Any = {
    transformed.eval(input)
  }

  override def dataType: DataType = transformed.dataType

  override def children: Seq[Expression] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = NativeExpression(handle, transformed)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev
}
