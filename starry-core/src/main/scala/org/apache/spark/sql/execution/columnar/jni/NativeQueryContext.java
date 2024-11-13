package org.apache.spark.sql.execution.columnar.jni;

import com.prx.starry.common.jni.NativeClass;

import java.util.concurrent.atomic.AtomicInteger;

public class NativeQueryContext extends NativeClass implements AutoCloseable {


  AtomicInteger ref = new AtomicInteger(0);

  static ThreadLocal<NativeQueryContext> queryContextThreadLocal = new ThreadLocal<>();

  public NativeQueryContext() {
    if (queryContextThreadLocal.get() != null) {
      throw new RuntimeException("Double queryContext");
    }
    setHandle(nativeCreate());
    queryContextThreadLocal.set(this);
  }


  protected native long nativeCreate();

  protected native void nativeRelease();

  protected native void nativeAttachCurrentThread();

  protected native void nativeDetachCurrentThread();

  public static void clear() {
    if (queryContextThreadLocal.get() != null && queryContextThreadLocal.get().ref.decrementAndGet() <= 0) {
      queryContextThreadLocal.get().close();
    }
    queryContextThreadLocal.remove();
  }


  public void attachCurrentThread() {
    ref.incrementAndGet();
    nativeAttachCurrentThread();
  }

  public void detachCurrentThread() {
    nativeDetachCurrentThread();
  }

  public static NativeQueryContext get() {
    return queryContextThreadLocal.get();
  }

  @Override
  protected void releaseInternal() {
    queryContextThreadLocal.set(null);
    detachCurrentThread();
    nativeRelease();
  }
}
