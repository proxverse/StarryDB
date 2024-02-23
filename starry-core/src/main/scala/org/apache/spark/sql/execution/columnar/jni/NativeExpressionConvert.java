package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch;
import org.apache.spark.sql.types.StructType;

public class NativeExpressionConvert {

  public static native String nativeToVeloxTypeString(String dt);

  public static native long nativeCreateFieldAccessTypedExprHanlde(String column, String dt);

  public static native long nativeCreateCastTypedExprHanlde(String dt, long column);

  public static native long nativeCreateCallTypedExprHanlde(String functionName, String resultType, long[] args);

  public static native long nativeCreateConstantTypedExprHanlde(String resultType, long vectorBatch);

  public static native long nativeEvalWithBatch(long expr, NativeColumnarBatch batch);


  public static VeloxColumnarBatch evalWithBatch(long expr, VeloxColumnarBatch batch, StructType structType) {
    NativeColumnVector rootVector = new NativeColumnVector(nativeEvalWithBatch(expr, batch.nativeObject()));
    return VeloxColumnarBatch.createFromRowVector(rootVector, structType);
  }


  public static native String nativeSerializeExpr(long expr);

  public static native long nativeDeserializeExpr(String exprJson);

  public static native void nativeReleaseHandle(long expr);

  public static native long nativeCreateConstantTypedExprHanlde(String resultType, NativeColumnVector vectorBatch);

}
