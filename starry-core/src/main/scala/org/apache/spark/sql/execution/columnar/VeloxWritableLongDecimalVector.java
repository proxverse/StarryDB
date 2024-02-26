package org.apache.spark.sql.execution.columnar;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.execution.columnar.jni.NativeColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import scala.Array;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;


/**
 * act as a view and won't close any vectors within
 */
public class VeloxWritableLongDecimalVector extends VeloxWritableColumnVector {

  static final boolean bigEndianPlatform = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  public VeloxWritableLongDecimalVector(int capacity, NativeColumnVector nativeColumnVector, DataType dataType) {
    super(capacity, nativeColumnVector, dataType);
  }

  public VeloxWritableLongDecimalVector(int capacity, DataType dataType) {
    super(capacity, dataType);
  }

  public VeloxWritableLongDecimalVector(NativeColumnVector nativeColumnVector, DataType dataType) {
    super(nativeColumnVector, dataType);
  }

  private byte[] padBytesArrayToUint128(byte[] bytes) {
    Preconditions.checkArgument(bytes.length <= 16, "bytes length exceeded 16");
    if (bytes.length == 16) {
      return bytes;
    }
    byte[] growTo = new byte[16];
    Array.copy(bytes, 0, growTo, 16 - bytes.length, bytes.length);
    return growTo;
  }

  private void alignByteOrder(byte[] array) {
    if (!bigEndianPlatform) {
      byte tmp;
      for(int i = 0, j = 15; i < j; i++, j--) {
        tmp = array[i];
        array[i] = array[j];
        array[j] = tmp;
      }
    }
  }

  @Override
  public void putDecimal(int rowId, Decimal value, int precision) {
    reserve(elementsAppended + 1);
    elementsAppended++;
    BigInteger unscaled = value.toJavaBigDecimal().unscaledValue();

    byte[] bytes;
    if (unscaled.signum() < 0) {
      BigInteger positive = unscaled.negate();
      bytes = padBytesArrayToUint128(positive.toByteArray());
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) ~bytes[i];
      }
      for (int i = bytes.length - 1, carry = 1; i >= 0 && carry == 1; i--) {
        int sum = (bytes[i] & 0xFF) + carry;
        bytes[i] = (byte) sum;
        carry = sum >> 8;
      }
    } else {
      bytes = padBytesArrayToUint128(unscaled.toByteArray());
    }

    alignByteOrder(bytes);
    Platform.copyMemory(
            bytes, Platform.BYTE_ARRAY_OFFSET, null, dataAddress + 16L * rowId, 16);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    if (dictionaryVector != null) {
      return dictionaryVector.getDecimal(dictionaryIds.getDictId(rowId), precision, scale);
    }
    byte[] array = new byte[16];
    Platform.copyMemory(null, dataAddress + 16L * rowId, array, Platform.BYTE_ARRAY_OFFSET, 16);
    alignByteOrder(array);

    BigDecimal javaDecimal = new BigDecimal(new BigInteger(array), scale);
    return Decimal.apply(javaDecimal, precision, scale);
  }
}

