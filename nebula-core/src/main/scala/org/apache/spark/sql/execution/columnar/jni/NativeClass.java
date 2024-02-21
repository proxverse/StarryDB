package org.apache.spark.sql.execution.columnar.jni;

public abstract class NativeClass implements AutoCloseable {
  long handle = 0;

  public void setHandle(long handle) {
    this.handle = handle;
    this.isRelease = false;
  }

  boolean isRelease = true;

  public long nativePTR() {
    return this.handle;
  }

  protected abstract void releaseInternal();

  public final void Release(){
    releaseInternal();
  }

  @Override
  public synchronized void close() {
    if (!isRelease) {
     Release();
      isRelease = true;
    }
  }

}
