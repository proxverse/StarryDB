package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch;
import org.apache.spark.sql.types.StructType;

public class NativeExpressionConvert {

  // json API
  public static native String nativeCreateFieldAccessTypedExpr(String column, String dt);

  public static native String nativeCreateConstantTypedExpr(String resultType, NativeColumnVector vectorBatch);

  public static native String nativeCreateCallTypedExpr(String functionName, String resultType, String[] args, boolean skipResolve);

  public static native long nativeEvalWithBatch(String expr, NativeColumnarBatch batch);

  public static VeloxColumnarBatch evalWithBatch(String expr, VeloxColumnarBatch batch, StructType structType) {
    NativeColumnVector rootVector = new NativeColumnVector(nativeEvalWithBatch(expr, batch.nativeObject()));
    return VeloxColumnarBatch.createFromRowVector(rootVector, structType);
  }

}
