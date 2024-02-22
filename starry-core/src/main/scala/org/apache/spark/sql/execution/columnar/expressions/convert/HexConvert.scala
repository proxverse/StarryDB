package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{LongType, StringType}

object Hexonvert extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = {
    val hex = expression.asInstanceOf[Hex]
    hex.child.dataType match {
      case LongType =>
        Hex(Cast(hex.child, StringType))
      case other => expression
    }
  }
}
