package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.types.StructType;

public class NativeColumnarBatch extends NativeClass {

  public NativeColumnarBatch(NativeColumnVector[] columns, int numRows) {
    setHandle(nativeCreate(columns, numRows));
  }

  public NativeColumnarBatch(long handle) {
    setHandle(handle);
  }

  private native long nativeCreate(NativeColumnVector[] columns, int numRows);

  private native void nativeSetNumRows(int numRows);


  private native long nativeRowVector();

  private native void nativeRelease();

  @Override
  protected void releaseInternal() {
    nativeRelease();
  }

  public void setNumRows(int numRows) {
    nativeSetNumRows(numRows);
  }


  public NativeColumnVector rowVector() {
    return new NativeColumnVector(nativeRowVector());
  }

  public void setSchema(StructType type) {
    nativeSetSchema(type.catalogString());
  }

  private native void nativeSetSchema(String s);
}
