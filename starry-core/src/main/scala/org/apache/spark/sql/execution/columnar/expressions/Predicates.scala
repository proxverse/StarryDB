/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf

class ColumnarOr(left: Expression, right: Expression)
    extends Or(left, right)
    with ColumnarExpression {
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ColumnarOr =
    new ColumnarOr(left = newLeft, right = newRight)
}

class ColumnarAnd(left: Expression, right: Expression)
    extends And(left, right)
    with ColumnarExpression {
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ColumnarAnd =
    new ColumnarAnd(left = newLeft, right = newRight)
}

class ColumnarEqualTo(left: Expression, right: Expression)
    extends EqualTo(left, right)
    with ColumnarExpression {
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ColumnarEqualTo =
    new ColumnarEqualTo(left = newLeft, right = newRight)
}

class ColumnarMultiply(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
    extends Multiply(left, right, failOnError)
    with ColumnarExpression {
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ColumnarMultiply =
    new ColumnarMultiply(left = newLeft, right = newRight, failOnError)
}

class ColumnarSubtract(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
    extends Subtract(left, right, failOnError)
    with ColumnarExpression {
  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ColumnarSubtract =
    new ColumnarSubtract(left = newLeft, right = newRight, failOnError)
}

class ColumnarNot(child: Expression) extends Not(child) with ColumnarExpression {
  override protected def withNewChildInternal(newChild: Expression): Not =
    new ColumnarNot(child = newChild)

}

class ColumnarIsNull(child: Expression)
    extends IsNull(child: Expression)
    with ColumnarExpression {
  override protected def withNewChildInternal(newChild: Expression): ColumnarIsNull =
    new ColumnarIsNull(child = newChild)
}
class ColumnarIsNotNull(child: Expression)
    extends IsNotNull(child: Expression)
    with ColumnarExpression {
  override protected def withNewChildInternal(newChild: Expression): ColumnarIsNotNull =
    new ColumnarIsNotNull(child = newChild)
}
