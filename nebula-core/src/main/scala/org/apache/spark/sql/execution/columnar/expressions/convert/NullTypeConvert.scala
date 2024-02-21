package org.apache.spark.sql.execution.columnar.expressions.convert

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.NullType

object NullTypeConvert extends ExpressionConvertTrait {
  override def beforeConvert(expression: Expression): Expression = expression match {
    case murmur3Hash: Murmur3Hash if murmur3Hash.children.exists(_.dataType.sameType(NullType))=>
      val dataType = murmur3Hash.children.filterNot(_.dataType.sameType(NullType)).head.dataType
      val newChildren = murmur3Hash.children.map { e =>
        if (e.dataType.sameType(NullType)) {
          Literal.create(null, dataType)
        } else {
          e
        }
      }
      murmur3Hash.withNewChildren(newChildren)
    case other => other

  }

}
