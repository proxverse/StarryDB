package org.apache.spark.sql.execution.columnar;

import org.apache.spark.sql.execution.columnar.jni.NativeColumnarVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;


/**
 * act as a view and won't close any vectors within
 */
public class VeloxWritableShortDecimalVector extends VeloxWritableColumnVector {

  public VeloxWritableShortDecimalVector(int capacity, NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(capacity, nativeColumnarVector, dataType);
  }

  public VeloxWritableShortDecimalVector(int capacity, DataType dataType) {
    super(capacity, dataType);
  }

  public VeloxWritableShortDecimalVector(NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(nativeColumnarVector, dataType);
  }

  @Override
  public void putDecimal(int rowId, Decimal value, int precision) {
    reserve(elementsAppended + 1);
    elementsAppended++;
    Platform.putLong(null, dataAddress + 8L * rowId, value.toUnscaledLong());
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    return Decimal.createUnsafe(getLong(rowId), precision, scale);
  }
}

