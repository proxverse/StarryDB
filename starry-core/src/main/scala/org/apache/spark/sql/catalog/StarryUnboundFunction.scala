package org.apache.spark.sql.catalog

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.functions.{AggregateFunction, BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.execution.columnar.extension.plan.VeloxTypeResolver
import org.apache.spark.sql.execution.columnar.jni.NativeExpressionConvert
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.concurrent.ConcurrentHashMap

case class StarryUnboundFunction(name: String) extends UnboundFunction {

  override def bind(inputType: StructType): BoundFunction = {
    StarryFunctionBounder.bind(name, inputType)
  }

  override def description(): String = "starry "

}

case class StarryScalarBoundFunction(
    inputTypes: Array[DataType],
    resultType: DataType,
    name: String)
    extends ScalarFunction[Any] {

  override def produceResult(input: InternalRow): Any = {
    throw new UnsupportedOperationException("")
  }
}

case class StarryAggBoundFunction(inputTypes: Array[DataType], resultType: DataType, name: String)
    extends AggregateFunction[Integer, Integer] {

  override def update(s: Integer, internalRow: InternalRow): Integer =
    throw new UnsupportedOperationException("")

  override def merge(s: Integer, s1: Integer): Integer =
    throw new UnsupportedOperationException("")

  override def produceResult(s: Integer): Integer = throw new UnsupportedOperationException("")

  override def newAggregationState(): Integer = throw new UnsupportedOperationException("")
}

object StarryFunctionBounder {

  case class FunctionCache(name: String, dataType: Seq[String])

  val functionCache = new ConcurrentHashMap[FunctionCache, BoundFunction]

  def bind(name: String, inputType: StructType): BoundFunction = {
    val inputs = inputType.fields.map(_.dataType.catalogString)
    val cacheKey = FunctionCache(name, inputs)
    if (functionCache.containsKey(cacheKey)) {
      functionCache.get(cacheKey)
    } else {
      var resutlJson = NativeExpressionConvert.nativeResolveFunction(name, inputs)
      if (resutlJson.equals("")) {
        resutlJson = NativeExpressionConvert.nativeResolveAggFunction(name, inputs)
        if (resutlJson.equals("")) {
          throw new UnsupportedOperationException()
        }
        val dt = VeloxTypeResolver.parseDataType(resutlJson)
        val name1 = StarryAggBoundFunction(inputType.fields.map(_.dataType), dt, name)
        functionCache.put(cacheKey, name1)
        return name1
      }
      // need set isDeterministic?
      val dt = VeloxTypeResolver.parseDataType(resutlJson)
      val name1 = StarryScalarBoundFunction(inputType.fields.map(_.dataType), dt, name)
      functionCache.put(cacheKey, name1)
      name1
    }
  }
}
