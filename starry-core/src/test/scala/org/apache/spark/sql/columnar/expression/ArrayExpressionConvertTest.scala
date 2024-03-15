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
package org.apache.spark.sql.columnar.expression

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.scalatest.Suite

class ArrayExpressionConvertTest extends ColumnarExpressionEvalHelper {
  self: Suite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def testIntegralDataTypes(testFunc: (Int => Any) => Unit): Unit = {
    testFunc(_.toByte)
    testFunc(_.toShort)
    testFunc(identity)
    testFunc(_.toLong)
  }

  test("GetArrayItem") {
    val typeA = ArrayType(StringType)
    val array = Literal.create(Seq("a", "b"), typeA)
    testIntegralDataTypes { convert =>
      checkEvaluation(GetArrayItem(array, Literal(convert(1))), "b")
    }
    val nullArray = Literal.create(null, typeA)
    val nullInt = Literal.create(null, IntegerType)
    checkEvaluation(GetArrayItem(nullArray, Literal(1)), null)
    checkEvaluation(GetArrayItem(array, nullInt), null)
    checkEvaluation(GetArrayItem(nullArray, nullInt), null)

    val nonNullArray = Literal.create(Seq(1), ArrayType(IntegerType, false))
    checkEvaluation(GetArrayItem(nonNullArray, Literal(0)), 1)

    val nestedArray = Literal.create(Seq(Seq(1)), ArrayType(ArrayType(IntegerType)))
    checkEvaluation(GetArrayItem(nestedArray, Literal(0)), Seq(1))
  }

  test("CreateArray") {
    val intSeq = Seq(5, 10, 15, 20, 25)
    val longSeq = intSeq.map(_.toLong)
    val byteSeq = intSeq.map(_.toByte)
    val strSeq = intSeq.map(_.toString)
    checkEvaluation(CreateArray(intSeq.map(Literal(_))), intSeq, EmptyRow)
    checkEvaluation(CreateArray(longSeq.map(Literal(_))), longSeq, EmptyRow)
    checkEvaluation(CreateArray(byteSeq.map(Literal(_))), byteSeq, EmptyRow)
    checkEvaluation(CreateArray(strSeq.map(Literal(_))), strSeq, EmptyRow)

    val intWithNull = intSeq.map(Literal(_)) :+ Literal.create(null, IntegerType)
    val longWithNull = longSeq.map(Literal(_)) :+ Literal.create(null, LongType)
    val byteWithNull = byteSeq.map(Literal(_)) :+ Literal.create(null, ByteType)
    val strWithNull = strSeq.map(Literal(_)) :+ Literal.create(null, StringType)
    checkEvaluation(CreateArray(intWithNull), intSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(longWithNull), longSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(byteWithNull), byteSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(strWithNull), strSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(Literal.create(null, IntegerType) :: Nil), null :: Nil)

    val array = CreateArray(
      Seq(
        Literal.create(intSeq, ArrayType(IntegerType, containsNull = false)),
        Literal.create(intSeq :+ null, ArrayType(IntegerType, containsNull = true))))
    assert(
      array.dataType ===
        ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = false))
    checkEvaluation(array, Seq(intSeq, intSeq :+ null))
  }

}
