
package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

case class NativeJsonExpression(native: String, original: Expression) extends ColumnarExpression {
  override def nullable: Boolean = original.nullable

  override def dataType: DataType = original.dataType

  override def children: Seq[Expression] = original :: Nil

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(original = children.head)

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException

  override def toString(): String = {
    val trimJson =
      if (native.length > 20) s"${native.take(20)}...${native.takeRight(5)}" else native
    s"native($trimJson, $original)"
  }

}
