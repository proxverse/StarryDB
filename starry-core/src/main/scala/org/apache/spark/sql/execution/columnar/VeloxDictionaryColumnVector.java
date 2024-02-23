package org.apache.spark.sql.execution.columnar;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.spark.sql.execution.columnar.jni.NativeColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class VeloxDictionaryColumnVector extends VeloxWritableColumnVector {
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

  long dataAddress;

  protected VeloxDictionaryColumnVector(NativeColumnVector nativeColumnVector, DataType type) {
    super(nativeColumnVector.capacity(), type);
    dictionaryIds = VeloxWritableColumnVector.bindVector(nativeColumnVector.dictIdVector(), DataTypes.IntegerType);
    dictionaryVector = VeloxWritableColumnVector.bindVector(nativeColumnVector.dictIdVector(), type);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return boolData;
  }


  @Override
  public byte getByte(int rowId) {
    return byteData;
  }


  @Override
  public short getShort(int rowId) {
    return shortData;
  }

  @Override
  public int getInt(int rowId) {
    return intData;
  }


  @Override
  public long getLong(int rowId) {
    return longData;
  }

  @Override
  public float getFloat(int rowId) {
    return floatData;
  }

  @Override
  public double getDouble(int rowId) {
    return doubleData;
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

  @Override
  public UTF8String getUTF8String(int rowId) {
    return stringData;
  }

  @Override
  public byte[] getBinary(int rowId) {
    return byteArrayData;
  }


}
