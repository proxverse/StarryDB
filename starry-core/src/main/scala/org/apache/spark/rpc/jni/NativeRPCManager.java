package org.apache.spark.rpc.jni;

import com.prx.starry.common.jni.NativeClass;

public class NativeRPCManager extends NativeClass {


  public NativeRPCManager() {
    setHandle(nativeCreate());
  }


  private native long nativeCreate();

  private native String nativeMemoryStatics();


  public String memoryStatics() {
    return nativeMemoryStatics();
  }


  native void nativeRelease();

  @Override
  protected void releaseInternal() {
    throw new UnsupportedOperationException("Memory manager can't release");
  }
}
