package org.apache.spark.sql.execution.columnar.expressions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.implicitCast
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SQLExecution, SubqueryExec}
import org.apache.spark.sql.execution.columnar.VeloxWritableColumnVector
import org.apache.spark.sql.execution.columnar.expressions.convert.ExpressionConvertMapping
import org.apache.spark.sql.execution.columnar.extension.plan.{VeloxRowToColumnConverter, VeloxTypeResolver}
import org.apache.spark.sql.execution.columnar.jni.{NativeExpressionConvert, NativeQueryContext}
import org.apache.spark.sql.types._

import java.util.regex.Pattern

object ExpressionConverter extends Logging {

  private val nullTypeTransform: PartialFunction[Expression, Expression] = {
    case cast: Cast if cast.child.dataType.sameType(NullType) =>
      Literal.create(null, cast.dataType)

    case arrayRepeat: ArrayRepeat if arrayRepeat.left.dataType.sameType(NullType) =>
      ArrayRepeat(Literal.create(null, StringType), arrayRepeat.right)
    case arrayDistinct: ArrayDistinct
        if arrayDistinct.child.dataType.asInstanceOf[ArrayType].elementType.sameType(NullType) =>
      ArrayDistinct(Literal.create(Seq(null, null, null), ArrayType(StringType)))
    case murmur3Hash: Murmur3Hash if murmur3Hash.children.exists(_.dataType.sameType(NullType)) =>
      val dataType = murmur3Hash.children.filterNot(_.dataType.sameType(NullType)).head.dataType
      val newChildren = murmur3Hash.children.map { e =>
        if (e.dataType.sameType(NullType)) {
          Literal.create(null, dataType)
        } else {
          e
        }
      }
      murmur3Hash.withNewChildren(newChildren)

    case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
      val children: Seq[Expression] = e.children.zip(e.inputTypes).map {
        case (in, expected) =>
          // If we cannot do the implicit cast, just use the original input.
          implicitCast(in, expected).getOrElse(in)
      }
      e.withNewChildren(children)

    case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
      // Convert NullType into some specific target type for ExpectsInputTypes that don't do
      // general implicit casting.
      val children: Seq[Expression] = e.children.zip(e.inputTypes).map {
        case (in, expected) =>
          if (in.dataType == NullType && !expected.acceptsType(NullType)) {
            Literal.create(null, expected.defaultConcreteType)
          } else {
            in
          }
      }
      e.withNewChildren(children)
    case other => other
  }

  def nativeField(name: String, expr: Expression): ColumnarExpression = {
    NativeJsonExpression(
      NativeExpressionConvert.nativeCreateFieldAccessTypedExpr(name, expr.dataType.catalogString),
      expr)
  }

  def nativeConstant(lit: Literal): ColumnarExpression = {
    // TODO use scalar valued constant ?
    val row = InternalRow.fromSeq(Seq(lit.value))
    val converter = VeloxRowToColumnConverter.getConverterForType(lit.dataType, true)
    val vector = VeloxWritableColumnVector.createVectorWithNative(1, lit.dataType)
    converter.append(row, 0, vector)
    val json = NativeExpressionConvert.nativeCreateConstantTypedExpr(
      lit.dataType.catalogString,
      vector.getNative)
    vector.close()
    NativeJsonExpression(json, lit)
  }

  def nativeCall(
      funcName: String,
      retType: DataType,
      args: Array[String],
      call: Expression,
      skipResolve: Boolean = false): ColumnarExpression = {
    NativeJsonExpression(
      NativeExpressionConvert
        .nativeCreateCallTypedExpr(funcName, retType.catalogString, args, skipResolve),
      call)
  }

  private def functionCall(expression: Expression): Expression = {
    val nativeFunctionName = ExpressionNamingProcess.lookupFunctionName(expression)
    val functionName = if (nativeFunctionName.isEmpty) {
      expression.prettyName
    } else {
      nativeFunctionName.get
    }
    expression match {
      case o if ExpressionConvertMapping.expressionsMap.contains(o.getClass) =>
        ExpressionConvertMapping.expressionsMap
          .apply(o.getClass)
          .convert(functionName, o)
      case other =>
        if (!expression.children.forall(_.isInstanceOf[NativeJsonExpression])) {
          logInfo(s"children has normal expression, skip transform ${expression}")
          return expression
        }
        try {
          nativeCall(
            functionName,
            expression.dataType,
            other.children.map(_.asInstanceOf[NativeJsonExpression].native).toArray,
            expression.withNewChildren(
              other.children.map(_.asInstanceOf[NativeJsonExpression].original)))
        } catch {
          case e =>
            logInfo("Error for convert expression", e)
            expression
        }
    }
  }

  private def beforeProcess(expression: Expression): Expression = expression.transformUp {
    case o if ExpressionConvertMapping.expressionsMap.contains(o.getClass) =>
      ExpressionConvertMapping.expressionsMap.apply(o.getClass).beforeConvert(o)
    case scalarSubquery: org.apache.spark.sql.execution.ScalarSubquery =>
      val value = scalarSubquery.plan
        .executeCollect()
        .head
        .get(0, scalarSubquery.dataType)
      new Literal(value, scalarSubquery.dataType)
    case other => other
  }

  def convertToNativeJson(expression: Expression, useAlias: Boolean = false): String = {
    convertToNative(expression, useAlias) match {
      case nativeJsonExpression: NativeJsonExpression => nativeJsonExpression.native
      case _ => throw new UnsupportedOperationException(s"Unsupported expression ${expression}")
    }
  }

  def convertToNative(expression: Expression, useAlias: Boolean = false): Expression = {
    beforeProcess(expression.transformUp(nullTypeTransform)).transformUp {
      case attr: AttributeReference =>
        val attrName = if (useAlias) {
          toNativeAttrIdName(attr)
        } else {
          attr.name.toLowerCase
        }
        nativeField(attrName, attr)
      case literal: Literal =>
        literal.dataType match {
          case NullType =>
            literal
          case FloatType if literal.value != null && literal.value.asInstanceOf[Float].isNaN =>
            nativeCall("float_nan", literal.dataType, Array[String](), literal)
          case FloatType
              if literal.value != null && literal.value.asInstanceOf[Float].isNegInfinity =>
            nativeCall("float_negative_infinity", literal.dataType, Array[String](), literal)
          case FloatType
              if literal.value != null && literal.value.asInstanceOf[Float].isPosInfinity =>
            nativeCall("float_infinity", literal.dataType, Array[String](), literal)
          case _: DoubleType
              if literal.value != null && literal.value.asInstanceOf[Double].isNaN =>
            nativeCall("nan", literal.dataType, Array[String](), literal)
          case _: DoubleType
              if literal.value != null && literal.value.asInstanceOf[Double].isNegInfinity =>
            nativeCall("negative_infinity", literal.dataType, Array[String](), literal)
          case _: DoubleType
              if literal.value != null && literal.value.asInstanceOf[Double].isPosInfinity =>
            nativeCall("infinity", literal.dataType, Array[String](), literal)
          case _ =>
            nativeConstant(literal)
        }
      case alias: Alias =>
        convertToNative(alias.child)
      case other =>
        functionCall(other)
    }
  }

  def nativeEvaluable(expression: Expression): Boolean = {
    convertToNative(expression).isInstanceOf[NativeJsonExpression]
  }

  def resolveNativeAggType(
      functionName: String,
      argsType: Array[String],
      step: String): DataType = {
    VeloxTypeResolver.parseDataType(
      NativeExpressionConvert.nativeResolveAggType(functionName, argsType, step))
  }

  def nativeAggregateExpressionJson(
      functionName: String,
      inputs: Array[String],
      rawInputs: Array[String],
      step: String,
      mask: String,
      sortingKeys: Array[String],
      sortOrders: Array[String],
      distinct: Boolean,
      useMergeFunc: Boolean): String = {
    NativeExpressionConvert.nativeBuildAggregationNode(
      functionName,
      inputs,
      rawInputs,
      step,
      mask,
      sortingKeys,
      sortOrders,
      distinct,
      useMergeFunc)
  }

  private val NON_ALPHANUMERIC_PATTERN = Pattern.compile("[^a-zA-Z0-9]")

  def toNativeAttrIdName(a: Attribute): String = {
    NON_ALPHANUMERIC_PATTERN.matcher(s"${a.name}_${a.exprId.id}").replaceAll("_").toLowerCase
  }

}
