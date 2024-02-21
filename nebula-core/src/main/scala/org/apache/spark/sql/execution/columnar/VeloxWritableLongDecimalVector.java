package org.apache.spark.sql.execution.columnar;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.spark.sql.execution.columnar.jni.NativeColumnarVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;


/**
 * act as a view and won't close any vectors within
 */
public class VeloxWritableLongDecimalVector extends VeloxWritableColumnVector {

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
    long low = parts[0].longValue();  // 低64位
    long high = parts[1].longValue(); // 高64位
    // 写入高64位和低64位，这里假设使用的是小端字节序
    Platform.putLong(null, dataAddress + 16L * rowId, low);
    Platform.putLong(null, dataAddress + 16L * rowId + 8, high);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    long low = Platform.getLong(null, dataAddress + 16L * rowId + 8);
    long high = Platform.getLong(null, dataAddress + 16L * rowId );
    // 使用BigInteger来处理这个128位的整数
    BigInteger highBigInt = BigInteger.valueOf(high);
    BigInteger lowBigInt = BigInteger.valueOf(low);
    if (high < 0) {
      highBigInt = highBigInt.add(BigInteger.ONE.shiftLeft(64));
    }
    // 将高64位移动到其位置
    highBigInt = highBigInt.shiftLeft(64);
    // 合并高位和低位以获得完整的128位整数
    BigInteger fullInt = highBigInt.or(lowBigInt);
    BigDecimal javaDecimal = new BigDecimal(fullInt, scale);
    return Decimal.apply(javaDecimal, precision, scale);
  }
}

