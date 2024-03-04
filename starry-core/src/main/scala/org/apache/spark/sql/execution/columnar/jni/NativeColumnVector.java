package org.apache.spark.sql.execution.columnar.jni;

import org.apache.spark.sql.types.DataType;

public class NativeColumnVector extends NativeClass {

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


  public NativeColumnVector(DataType dataType, int capacity) {
    setHandle(nativeCreate(dataType.catalogString(), capacity));
  }

  public NativeColumnVector(long handle) {
    setHandle(handle);
  }


  private native long nativeCreate(String dataType, int capacity);

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

  private native long nativeDictVector();

  private native long nativeDictionaryIdVector();

  private native String nativeEncoding();

  private native byte[] nativeSerialize();


  private static native long nativeDeserialize(byte[] json);

  private native long nativeCopy();


  public NativeColumnVector reserveDictionaryIds(int capacity) {
    return new NativeColumnVector(nativeReserveDictionaryIds(capacity));
  }


  public void reserve(int capacity) {
    nativeReserve(capacity);
  }


  public NativeColumnVector createDictionaryVector(int capacity) {
    return new NativeColumnVector(nativeCreateDictionaryVector(capacity));
  }


  public void removeDictionaryVector() {
    nativeRemoveDictionaryVector();
  }


  public NativeColumnVector newChildWithIndex(int index) {
    return new NativeColumnVector(nativeNewChildWithIndex(index));
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

  public NativeColumnVector valueVector() {
    return new NativeColumnVector(nativeValueVector());
  }

  public NativeColumnVector dictVector() {
    return new NativeColumnVector(nativeDictVector());
  }

  public NativeColumnVector dictIdVector() {
    return new NativeColumnVector(nativeDictionaryIdVector());
  }


  public byte[] serialize() {
    return nativeSerialize();
  }

  public static NativeColumnVector deserialize(byte[] json) {
    return new NativeColumnVector(nativeDeserialize(json));
  }

  public NativeColumnVector cache() {
    return new NativeColumnVector(nativeCopy());
  }


  @Override
  protected void releaseInternal() {
    nativeRelease();
  }


}
