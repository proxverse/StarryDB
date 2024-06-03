package org.apache.spark.sql.execution.columnar.jni;


import java.util.List;

import com.prx.starry.common.jni.NativeClass;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch;
import org.apache.spark.sql.execution.columnar.extension.vector.ColumnarBatchInIterator;

public class NativeColumnarExecution extends NativeClass {
  List<Attribute> resultAttrs;

  public NativeColumnarExecution(List<Attribute> resultAttrs, String memoryPool) {
    setHandle(nativeCreate(memoryPool));
    this.resultAttrs = resultAttrs;
  }

  private native long nativeCreate(String memoryPool);

  private native void nativeRelease();

  private native boolean nativeHasNext();

  private native long nativeNext();

  private native String nativeMetrics();

  private native void nativeInit(String planJson, String[] nodeIds, ColumnarBatchInIterator[] batchItr, String conf);


  VeloxColumnarBatch current;

  public boolean hasNext() {
    return nativeHasNext();
  }

  public VeloxColumnarBatch next() {
    if (current != null) {
      current.close();
    }
    current = VeloxColumnarBatch.createFromRowVectorHandle(nativeNext(), resultAttrs);
    return current;
  }

  public String getMetrics() {
    return nativeMetrics();
  }


  public void init(String planJson, String[] nodeIds, ColumnarBatchInIterator[] batchItr, String conf) {
    nativeInit(planJson, nodeIds, batchItr, conf);
  }

  @Override
  protected void releaseInternal() {
    if (current != null) {
      current.close();
    }
    nativeRelease();
  }
}
