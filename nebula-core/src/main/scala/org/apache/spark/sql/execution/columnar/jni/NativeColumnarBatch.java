package org.apache.spark.sql.execution.columnar.jni;

public class NativeColumnarBatch extends NativeClass {

  public NativeColumnarBatch(NativeColumnarVector[] columns,int numRows) {
    setHandle(nativeCreate(columns, numRows));
  }

  public NativeColumnarBatch(long handle) {
    setHandle(handle);
  }

  private native long nativeCreate(NativeColumnarVector[] columns, int numRows);

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


  public NativeColumnarVector rowVector() {
    return new NativeColumnarVector(nativeRowVector());
  }
}
