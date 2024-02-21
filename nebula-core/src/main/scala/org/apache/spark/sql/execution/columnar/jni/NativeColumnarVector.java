package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.types.DataType;

public class NativeColumnarVector extends NativeClass {

  public void reset() {
    nativeReset();
  }

  public enum DataTypeEnum {
    DATA(0),

    NULL(1),
    OFFSETS(2),
    LENGTHS(3),
    CONSTANTS(4);

    DataTypeEnum(int type) {
      this.type = type;
    }

    final int type;
  }


  public NativeColumnarVector(DataType dataType) {
    setHandle(nativeCreate(dataType.catalogString()));
  }

  public NativeColumnarVector(long handle) {
    setHandle(handle);
  }


  private native long nativeCreate(String dataType);

  private native long nativeNewChildWithIndex(int index);

  private native int nativeCapacity();

  private native long nativeDataAddress(int type);

  private native void nativeRelease();

  private native void nativeReset();

  private native boolean nativeHasNulls();

  private native long nativeReserveDictionaryIds(int capacity);

  private native void nativeReserve(int capacity);

  private native long nativeCreateDictionaryVector(int capacity);

  private native void nativeRemoveDictionaryVector();

  private native long nativeAllocateStringData(int length);


  private native long nativeValueVector();

  private native long nativeDictionaryIdVector();

  private native String nativeEncoding();

  private native byte[] nativeSerialize();


  private static native long nativeDeserialize(byte[] json);


  public NativeColumnarVector reserveDictionaryIds(int capacity) {
    return new NativeColumnarVector(nativeReserveDictionaryIds(capacity));
  }


  public void reserve(int capacity) {
    nativeReserve(capacity);
  }


  public NativeColumnarVector createDictionaryVector(int capacity) {
    return new NativeColumnarVector(nativeCreateDictionaryVector(capacity));
  }


  public void removeDictionaryVector() {
    nativeRemoveDictionaryVector();
  }


  public NativeColumnarVector newChildWithIndex(int index) {
    return new NativeColumnarVector(nativeNewChildWithIndex(index));
  }

  public int capacity() {
    return nativeCapacity();
  }

  public long dataAddress(DataTypeEnum dataTypeEnum) {
    return nativeDataAddress(dataTypeEnum.type);
  }

  public boolean mayHasNulls() {
    return nativeHasNulls();
  }


  public long allocateStringData(int length) {
    return nativeAllocateStringData(length);
  }

  public String encoding() {
    return nativeEncoding();
  }

  public NativeColumnarVector valueVector() {
    return new NativeColumnarVector(nativeValueVector());
  }

  public NativeColumnarVector dictIdVector() {
    return new NativeColumnarVector(nativeDictionaryIdVector());
  }


  public byte[] serialize() {
    return  nativeSerialize();
  }

  public static NativeColumnarVector deserialize(byte[] json) {
    return new NativeColumnarVector(nativeDeserialize(json));
  }


  @Override
  protected void releaseInternal() {
    nativeRelease();
  }


}
