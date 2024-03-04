package org.apache.spark.sql.execution.columnar;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.spark.sql.execution.columnar.jni.NativeColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public class VeloxConstantsVector extends VeloxWritableColumnVector {
  /**
   * Sets up the data type of this column vector.
   *
   * @param type
   */

  boolean isNull;

  private byte nullData;

  private byte byteData;

  private boolean boolData;

  private short shortData;
  private int intData;
  private long longData;
  private float floatData;
  private double doubleData;
  private UTF8String stringData;
  private byte[] byteArrayData;
  private ColumnarArray arrayData;
  private ColumnarMap mapData;

  private Decimal decimalData;

  VeloxWritableColumnVector complexTypeVector;

  NativeColumnVector nativeColumnVector;

  long dataAddress;

  protected VeloxConstantsVector(NativeColumnVector nativeColumnVector, DataType type) {
    super(nativeColumnVector.capacity(), type);
    this.nativeColumnVector = nativeColumnVector;
    isNull = nativeColumnVector.mayHasNulls();
    if (type instanceof ArrayType || type instanceof MapType || type instanceof StructType) {
      complexTypeVector = VeloxWritableColumnVector.bindVector(nativeColumnVector.valueVector(), type);
    } else {
      dataAddress = nativeColumnVector.dataAddress(NativeColumnVector.DataTypeEnum.CONSTANTS);
      if (type instanceof StringType || type instanceof BinaryType) {
        int length = Platform.getInt(null, dataAddress);
        if (length <= 12) {
          stringData = UTF8String.fromAddress(null, dataAddress + 4, length);
        } else {
          long address = Platform.getLong(null, dataAddress + 8);
          stringData = UTF8String.fromAddress(null, address, length);
        }
        byteArrayData = stringData.getBytes();
      } else if (type instanceof ByteType) {
        byteData = Platform.getByte(null, dataAddress);
      } else if (type instanceof ShortType) {
        shortData = Platform.getShort(null, dataAddress);
      } else if (type instanceof IntegerType || type instanceof DateType || DecimalType.is32BitDecimalType(type) || type instanceof YearMonthIntervalType) {
        intData = Platform.getInt(null, dataAddress);
      } else if (type instanceof LongType || DecimalType.is64BitDecimalType(type) || type instanceof DayTimeIntervalType) {
        longData = Platform.getLong(null, dataAddress);
      } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
        long sec = Platform.getLong(null, dataAddress);
        long nano = Platform.getLong(null, dataAddress + 8);
        longData = sec * 1000000 + nano;
      } else if (type instanceof FloatType) {
        floatData = Platform.getFloat(null, dataAddress);
      } else if (type instanceof DoubleType) {
        doubleData = Platform.getDouble(null, dataAddress);
      } else if (type instanceof BooleanType) {
        int bitIndex = 0 % 8;  // 计算在该字节中的位位置
        byte currentByte = Platform.getByte(null, dataAddress);
        boolData = (currentByte & (1 << bitIndex)) != 0;
      } else if (type instanceof DecimalType) {
        DecimalType decimalType = (DecimalType) type;
        if (decimalType.precision() <= Decimal.MAX_LONG_DIGITS()) {
          decimalData = Decimal.createUnsafe(getLong(0), decimalType.precision(), decimalType.scale());
        } else {
          byte[] bytes = UTF8String.fromAddress(null, dataAddress + 16L * 0, 16).getBytes();
          BigInteger bigInteger = new BigInteger(bytes);
          BigDecimal javaDecimal = new BigDecimal(bigInteger, decimalType.scale());
          decimalData = Decimal.apply(javaDecimal, decimalType.precision(), decimalType.scale());
        }
      } else {
        throw new RuntimeException("Unhandled " + type);
      }
    }


  }

  @Override
  public void close() {
    if (!isClosed) {
      if (complexTypeVector != null) {
        complexTypeVector.close();
      }
      nativeColumnVector.close();
      super.close();
    }
  }

  @Override
  public boolean hasNull() {
    return isNull;
  }

  @Override
  public int numNulls() {
    return hasNull() ? capacity : 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return isNull;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return boolData;
  }

  /**
   * Sets the boolean `value` for all rows
   */
  public void setBoolean(boolean value) {
    byteData = (byte) ((value) ? 1 : 0);
    super.putBoolean(0, value);
  }

  @Override
  public byte getByte(int rowId) {
    return byteData;
  }

  /**
   * Sets the byte `value` for all rows
   */
  public void setByte(byte value) {
    byteData = value;
  }

  @Override
  public short getShort(int rowId) {
    return shortData;
  }

  /**
   * Sets the short `value` for all rows
   */
  public void setShort(short value) {
    shortData = value;
    putShort(0, value);
  }

  @Override
  public int getInt(int rowId) {
    return intData;
  }

  /**
   * Sets the int `value` for all rows
   */
  public void setInt(int value) {
    intData = value;
    putInt(0, value);
  }

  @Override
  public long getLong(int rowId) {
    return longData;
  }

  /**
   * Sets the long `value` for all rows
   */
  public void setLong(long value) {
    longData = value;
    putLong(0, value);
  }

  @Override
  public float getFloat(int rowId) {
    return floatData;
  }

  /**
   * Sets the float `value` for all rows
   */
  public void setFloat(float value) {
    floatData = value;
    putFloat(0, value);
  }

  @Override
  public double getDouble(int rowId) {
    return doubleData;
  }

  /**
   * Sets the double `value` for all rows
   */
  public void setDouble(double value) {
    doubleData = value;
    putDouble(0, value);
  }

  @Override
  public int getArrayLength(int rowId) {
    return complexTypeVector.getArrayLength(0);
  }

  @Override
  public int getArrayOffset(int rowId) {
    return complexTypeVector.getArrayOffset(0);
  }

  @Override
  public WritableColumnVector arrayData() {
    return complexTypeVector.getChild(0);
  }

  /**
   * Sets the `ColumnarArray` `value` for all rows
   */
  public void setArray(ColumnarArray value) {
    throw new UnsupportedOperationException();
  }



  /**
   * Sets the `ColumnarMap` `value` for all rows
   */
  public void setMap(ColumnarMap value) {
    mapData = value;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    // copy and modify from WritableColumnVector
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  /**
   * Sets the `Decimal` `value` with the precision for all rows
   */
  public void setDecimal(Decimal value, int precision) {
    // copy and modify from WritableColumnVector
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      setInt((int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(0, value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      byte[] byteArray = bigInteger.toByteArray();
      Platform.copyMemory(byteArray, Platform.BYTE_ARRAY_OFFSET, null, dataAddress + 16L * 0, byteArray.length);
    }
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return stringData;
  }

  /**
   * Sets the `UTF8String` `value` for all rows
   */
  public void setUtf8String(UTF8String value) {
    stringData = value;
    byte[] bytes = value.getBytes();
    putByteArray(0, bytes, 0, bytes.length);

  }

  /**
   * Sets the byte array `value` for all rows
   */
  private void setByteArray(byte[] value) {
    byteArrayData = value;
    putByteArray(0, value, 0, value.length);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return byteArrayData;
  }

  /**
   * Sets the binary `value` for all rows
   */
  public void setBinary(byte[] value) {
    setByteArray(value);
  }

  @Override
  public WritableColumnVector getChild(int ordinal) {
    return complexTypeVector.getChild(ordinal);
  }


}
