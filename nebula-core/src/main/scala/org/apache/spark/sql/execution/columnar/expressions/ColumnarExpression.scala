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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColumnarExpression extends Expression with Serializable {

  /**
   * Returns true if this expression supports columnar processing through [[columnarEval]].
   */
  def supportsColumnar: Boolean = true

  /**
   * Returns the result of evaluating this expression on the entire
   * [[org.apache.spark.sql.vectorized.ColumnarBatch]]. The result of
   * calling this may be a single [[org.apache.spark.sql.vectorized.ColumnVector]] or a scalar
   * value. Scalar values typically happen if they are a part of the expression i.e. col("a") + 100.
   * In this case the 100 is a [[org.apache.spark.sql.catalyst.expressions.Literal]] that
   * [[org.apache.spark.sql.catalyst.expressions.Add]] would have to be able to handle.
   *
   * By convention any [[org.apache.spark.sql.vectorized.ColumnVector]] returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it into
   * a [[org.apache.spark.sql.vectorized.ColumnarBatch]] and closing the batch or by closing the
   * vector directly if it is a temporary value.
   */
  def columnarEval(batch: ColumnarBatch): Any = {
    throw new IllegalStateException(
      s"Internal Error ${this.getClass} has column support mismatch")
  }

  // We need to override equals because we are subclassing a case class
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarExpression]
  }

  override def hashCode(): Int = super.hashCode()

  def collectColumnSQL(sqls: java.util.ArrayList[String]): Unit = {
    sqls.add(sql)
  }
}
