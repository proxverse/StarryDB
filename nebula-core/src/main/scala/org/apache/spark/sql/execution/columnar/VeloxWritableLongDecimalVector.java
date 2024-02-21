package org.apache.spark.sql.execution.columnar;

import org.apache.spark.sql.execution.columnar.jni.NativeColumnarVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;


/**
 * act as a view and won't close any vectors within
 */
public class VeloxWritableLongDecimalVector extends VeloxWritableColumnVector {

  static final boolean bigEndianPlatform = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  public VeloxWritableLongDecimalVector(int capacity, NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(capacity, nativeColumnarVector, dataType);
  }

  public VeloxWritableLongDecimalVector(int capacity, DataType dataType) {
    super(capacity, dataType);
  }

  public VeloxWritableLongDecimalVector(NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(nativeColumnarVector, dataType);
  }

  @Override
  public void putDecimal(int rowId, Decimal value, int precision) {
    reserve(elementsAppended + 1);
    elementsAppended++;
    BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();

    BigInteger[] parts = bigInteger.divideAndRemainder(BigInteger.ONE.shiftLeft(64));
    long high = parts[0].longValue();
    long low = parts[1].longValue();
    if (bigEndianPlatform) {
      Platform.putLong(null, dataAddress + 16L * rowId, high);
      Platform.putLong(null, dataAddress + 16L * rowId + 8, low);
    } else {
      Platform.putLong(null, dataAddress + 16L * rowId, low);
      Platform.putLong(null, dataAddress + 16L * rowId + 8, high);
    }
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    byte[] array = new byte[16];
    Platform.copyMemory(null, dataAddress + 16L * rowId, array, Platform.BYTE_ARRAY_OFFSET, 16);

    if (!bigEndianPlatform) {
      byte tmp;
      for(int i = 0, j = 15; i < j; i++, j--) {
        tmp = array[i];
        array[i] = array[j];
        array[j] = tmp;
      }
    }
    BigDecimal javaDecimal = new BigDecimal(new BigInteger(array), scale);
    return Decimal.apply(javaDecimal, precision, scale);
  }
}

