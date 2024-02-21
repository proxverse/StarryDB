package org.apache.spark.sql.execution.columnar;

import org.apache.spark.sql.execution.columnar.jni.NativeColumnarVector;
import org.apache.spark.sql.types.DataType;


/**
 * act as a view and won't close any vectors within
 */
public class VeloxWritableStringVector extends VeloxWritableColumnVector {

  public VeloxWritableStringVector(int capacity, NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(capacity, nativeColumnarVector, dataType);
  }

  public VeloxWritableStringVector(int capacity, DataType dataType) {
    super(capacity, dataType);
  }

  public VeloxWritableStringVector(NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(nativeColumnarVector, dataType);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException("String type can't support appendByteArray, use putByteArray");
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    throw new UnsupportedOperationException("String type can't support appendByteArray, use putByteArray");
  }

  @Override
  public int getArrayLength(int rowId) {
    throw new UnsupportedOperationException("String type can't support getArrayLength");
  }
}

