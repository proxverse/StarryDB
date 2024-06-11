package org.apache.spark.sql.execution.columnar;

import org.apache.spark.sql.execution.columnar.jni.NativeColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.Platform;


/**
 * act as a view and won't close any vectors within
 */
public class VeloxWritableTimestampVector extends VeloxWritableColumnVector {

  public VeloxWritableTimestampVector(int capacity, NativeColumnVector nativeColumnVector, DataType dataType) {
    super(capacity, nativeColumnVector, dataType);
  }

  public VeloxWritableTimestampVector(int capacity, DataType dataType) {
    super(capacity, dataType);
  }

  public VeloxWritableTimestampVector(NativeColumnVector nativeColumnVector, DataType dataType) {
    super(nativeColumnVector, dataType);
  }

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(null, dataAddress + 16L * rowId, value / 1000000);
    Platform.putLong(null, dataAddress + 16L * rowId + 8, (value % 1000000) * 1000);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      for (int i = 0; i < count; i++, rowId++) {
        putLong(rowId, Platform.getLong(src, srcIndex + Platform.BYTE_ARRAY_OFFSET + 8L * i));
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    long[] array = new long[count];
    for (int i = rowId; i < rowId + count; i++) {
      array[i -rowId] = getLong(i);
    }
    return array;
  }

  @Override
  public long getLong(int rowId) {
    if (dictionaryVector != null) {
      return dictionaryVector.getLong(dictionaryIds.getDictId(rowId));
    }
    long sec = Platform.getLong(null, dataAddress + 16L * rowId);
    long micros = Platform.getLong(null, dataAddress + 16L * rowId + 8) / 1000;
    return sec * 1000000 + micros;
  }
}
