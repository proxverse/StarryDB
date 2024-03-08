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

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, TypeUtils}
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, LongType}



@ExpressionDescription(
  usage = """
    _FUNC_(array, element) - Returns the (1-based) index of the first element of the array as long.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(3, 2, 1), 1);
       3
  """,
  group = "array_funcs",
  since = "2.4.0"
)
case class ArrayPositionWithInstance(column: Expression, element: Expression, instance: Expression)
  extends TernaryExpression
  with ImplicitCastInputTypes
  with NullIntolerant {

  override def first: Expression = column
  override def second: Expression = element

  override def third: Expression = instance

  @transient private lazy val ordering: Ordering[Any] = column.dataType match {
    case ArrayType(elementType, _) =>
      TypeUtils.getInterpretedOrdering(elementType)
    case _ =>
      throw new UnsupportedOperationException(
        s"Expect array type but got ${column.dataType} for $column")
  }

  override def dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = {
    (column.dataType, element.dataType, instance.dataType) match {
      case (ArrayType(e1, hasNull), e2, instance) =>
        TypeCoercion.findTightestCommonType(e1, e2) match {
          case Some(dt) => Seq(ArrayType(dt, hasNull), dt, LongType)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (column.dataType, element.dataType) match {
      case (ArrayType(e1, _), e2) if e1.sameType(e2) =>
        TypeUtils.checkForOrderingExpr(e2, s"function $prettyName")
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${ArrayType.simpleString} followed by a value with same element type, but it's " +
          s"[${column.dataType.catalogString}, ${element.dataType.catalogString}].")
    }
  }

  override def nullSafeEval(arr: Any, value: Any, instance: Any): Any = {
    val data = arr
      .asInstanceOf[ArrayData]
    if (instance.asInstanceOf[Long] > 0) {
      var index = 0
      var currentInstance = 0L
      while (index < data.numElements()) {
        if (!data.isNullAt(index)) {
          if (ordering.equiv(data.get(index, element.dataType), value)) {
            currentInstance += 1
            if (currentInstance == instance.asInstanceOf[Long]) {
              return (index + 1).toLong
            }
          }
        }
        index += 1
      }
    } else {
      var index = data.numElements() - 1
      val absInstance = Math.abs(instance.asInstanceOf[Long])
      var currentInstance = 0L
      while (index >= 0) {
        if (!data.isNullAt(index)) {
          if (ordering.equiv(data.get(index, element.dataType), value)) {
            currentInstance += 1
            if (currentInstance == absInstance) {
              return (index + 1).toLong
            }
          }
        }
        index -= 1
      }
    }
    0L
  }

  override def prettyName: String = "array_position_instance"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ins = instance.eval().asInstanceOf[Long]
    nullSafeCodeGen(
      ctx,
      ev,
      (arr, value, instance) => {
        val pos = ctx.freshName("arrayPosition")
        val i = ctx.freshName("i")
        val currentInstance = ctx.freshName("currentInstance")
        val getValue = CodeGenerator.getValue(arr, element.dataType, i)
        if (ins > 0) {
          s"""
             |int $pos = 0;
             |int $currentInstance = 0;
             |for (int $i = 0; $i < $arr.numElements(); $i ++) {
             |  if (!$arr.isNullAt($i) && ${ctx.genEqual(element.dataType, value, getValue)}) {
             |    $currentInstance ++;
             |    if ($currentInstance == $instance) {
             |      $pos = $i + 1;
             |       break;
             |     }
             |  }
             |}
             |${ev.value} = (long) $pos;
       """.stripMargin
        } else {
          val absInstance = Math.abs(ins)
          s"""
             |int $pos = 0;
             |int $currentInstance = 0;
             |for (int $i = $arr.numElements() - 1; $i >= 0; $i --) {
             |  if (!$arr.isNullAt($i) && ${ctx.genEqual(element.dataType, value, getValue)}) {
             |    $currentInstance ++;
             |    if ($currentInstance == $absInstance) {
             |      $pos = $i + 1;
             |       break;
             |     }
             |  }
             |}
             |${ev.value} = (long) $pos;
         """.stripMargin
        }
      }
    )
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression,
      newInstance: Expression): ArrayPositionWithInstance =
    copy(column = newLeft, element = newRight, instance = newInstance)
}
