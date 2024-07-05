package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero

object NormalizedConvert extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = {
    val knownFloatingPointNormalized = expression.asInstanceOf[KnownFloatingPointNormalized]
    knownFloatingPointNormalized.child match {
      case c: NormalizeNaNAndZero =>
        c.child
      case other => knownFloatingPointNormalized.child
    }
  }
}
