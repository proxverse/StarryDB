package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch;
import org.apache.spark.sql.types.StructType;

public class NativeExpression extends NativeClass {


  public NativeExpression(long handle) {
    super();
    setHandle(handle);
  }

  private static native long nativeCreateFieldAccessTypedExpr(String column, String dt);


  private static native long nativeDeserializeExpr(String exprJson);


  private native long nativeCreateCallTypedExprHanlde(String functionName, String resultType, NativeExpression[] args);



  private native long eval(NativeColumnarVector nativeColumnarBatch);

  private native void nativeRelease();


  static NativeExpression fromFieldAccess(String columnName, String dt) {
    return new NativeExpression(nativeCreateFieldAccessTypedExpr(columnName, dt));
  }

  static NativeExpression fromJson(String json) {
    return new NativeExpression(nativeDeserializeExpr(json));
  }


  public VeloxColumnarBatch eval(NativeColumnarVector batch, StructType structType) {
    return VeloxColumnarBatch.createFromRowVector(new NativeColumnarVector(eval(batch)), structType);
  }

  @Override
  protected void releaseInternal() {
    nativeRelease();
  }
}
