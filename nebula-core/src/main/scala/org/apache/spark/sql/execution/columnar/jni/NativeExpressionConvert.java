package org.apache.spark.sql.execution.columnar.jni;

import java.lang.annotation.Native;

public class NativeExpressionConvert {

  public static native String nativeToVeloxTypeString(String dt);
  public static native long nativeCreateFieldAccessTypedExprHanlde(String column, String dt);

  public static native long nativeCreateCastTypedExprHanlde(String dt, long column);

  public static native long nativeCreateCallTypedExprHanlde(String functionName, String resultType, long[] args);

  public static native long nativeCreateConstantTypedExprHanlde(String resultType, long vectorBatch);

  public static native long nativeEvalWithBatch(long expr, long batch);

  public static native String nativeSerializeExpr(long expr);

  public static native long nativeDeserializeExpr(String exprJson);

  public static native void nativeReleaseHandle(long expr);

  public static native long nativeCreateConstantTypedExprHanlde(String resultType, NativeColumnarVector vectorBatch);

}
