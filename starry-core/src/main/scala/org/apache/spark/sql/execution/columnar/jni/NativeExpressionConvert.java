package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch;
import org.apache.spark.sql.execution.columnar.extension.plan.VeloxTypeResolver;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public class NativeExpressionConvert {

  // json API
  public static native String nativeCreateFieldAccessTypedExpr(String column, String dt);

  public static native String nativeCreateConstantTypedExpr(String resultType, NativeColumnVector vectorBatch);

  public static native String nativeCreateCallTypedExpr(String functionName, String resultType, String[] args, boolean skipResolve);

  public static native long nativeEvalWithBatch(String expr, NativeColumnarBatch batch);

  public static native String nativeBuildAggregationNode(String functionName,
                                                         String[] inputs,
                                                         String[] rawInputs,
                                                         String step,
                                                         String mask,
                                                         String[] sortingKeys,
                                                         String[] sortOrders,
                                                         boolean distinct,
                                                         boolean useMergeFunc);

  public static VeloxColumnarBatch evalWithBatch(String expr, VeloxColumnarBatch batch, StructType structType) {
    NativeColumnVector rootVector = new NativeColumnVector(nativeEvalWithBatch(expr, batch.nativeObject()));
    return VeloxColumnarBatch.createFromRowVector(rootVector, structType);
  }

  public static native String nativeResolveAggType(String functionName,
                                                   String[] argsType,
                                                   String step);

  public static native String nativeResolveFunction(String functionName,
                                                    String[] argsType);

  public static native String nativeResolveAggFunction(String functionName,
                                                       String[] argsType);

  public static native boolean nativeFunctionExists(String functionName);

}
