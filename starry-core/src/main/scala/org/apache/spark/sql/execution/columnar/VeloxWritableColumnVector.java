package org.apache.spark.sql.execution.columnar;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.spark.sql.execution.columnar.jni.NativeColumnarVector;
import org.apache.spark.sql.execution.datasources.parquet.ParquetDictionary;
import org.apache.spark.sql.execution.vectorized.Dictionary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public class VeloxWritableColumnVector extends WritableColumnVector {
  static final boolean bigEndianPlatform = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
  long dataAddress;
  long nullAddress;

  long offsetAddress;

  long lengthAddress;

  int nullElement = 0;

  boolean isClosed = false;

  VeloxWritableColumnVector dictionaryVector = null;
  boolean isBoolean = false;

  boolean hasNull = false;

  NativeColumnarVector nativeColumnarVector;

  String operationType;

  public static VeloxWritableColumnVector createVector(int capacity, DataType dataType) {
    if (dataType.sameType(DataTypes.TimestampType)) {
      return new VeloxWritableTimestampVector(capacity, dataType);
    } else if (dataType.sameType(DataTypes.StringType) || dataType.sameType(DataTypes.BinaryType)) {
      return new VeloxWritableStringVector(capacity, dataType);
    } else if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      if (decimalType.precision() <= Decimal.MAX_LONG_DIGITS()) {
        return new VeloxWritableShortDecimalVector(capacity, dataType);
      } else {
        return new VeloxWritableLongDecimalVector(capacity, dataType);
      }
    } else {
      return new VeloxWritableColumnVector(capacity, dataType);
    }
  }


  public static VeloxWritableColumnVector createVector(int capacity, NativeColumnarVector nativeColumnarVector, DataType dataType) {
    if (dataType.sameType(DataTypes.TimestampType)) {
      return new VeloxWritableTimestampVector(capacity, nativeColumnarVector, dataType);
    } else if (dataType.sameType(DataTypes.StringType) || dataType.sameType(DataTypes.BinaryType)) {
      return new VeloxWritableStringVector(capacity, nativeColumnarVector, dataType);
    } else if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      if (decimalType.precision() <= Decimal.MAX_LONG_DIGITS()) {
        return new VeloxWritableShortDecimalVector(capacity, nativeColumnarVector, dataType);
      } else {
        return new VeloxWritableLongDecimalVector(capacity, nativeColumnarVector, dataType);
      }
    } else {
      return new VeloxWritableColumnVector(capacity, nativeColumnarVector, dataType);
    }
  }

  public static VeloxWritableColumnVector bindVector(NativeColumnarVector nativeColumnarVector, DataType dataType) {
    String encoding = nativeColumnarVector.encoding();
    if ("CONSTANT".equals(encoding)) {
      return new VeloxConstantsVector(nativeColumnarVector, dataType);
    } else {
      if (dataType.sameType(DataTypes.TimestampType)) {
        return new VeloxWritableTimestampVector(nativeColumnarVector, dataType);
      } else if (dataType.sameType(DataTypes.StringType) || dataType.sameType(DataTypes.BinaryType)) {
        return new VeloxWritableStringVector(nativeColumnarVector, dataType);
      } else if (dataType instanceof DecimalType) {
        DecimalType decimalType = (DecimalType) dataType;
        if (decimalType.precision() <= Decimal.MAX_LONG_DIGITS()) {
          return new VeloxWritableShortDecimalVector(nativeColumnarVector, dataType);
        } else {
          return new VeloxWritableLongDecimalVector(nativeColumnarVector, dataType);
        }
      } else {
        return new VeloxWritableColumnVector(nativeColumnarVector, dataType);
      }
    }
  }


  // from  complex type
  public VeloxWritableColumnVector(int capacity, NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(capacity, dataType);
    this.nativeColumnarVector = nativeColumnarVector;
    if (!(dataType instanceof ArrayType || dataType instanceof MapType)) {
      nativeColumnarVector.reserve(capacity);
    }
    this.operationType = "WRITER";
    this.capacity = nativeColumnarVector.capacity();
    initInternal();
  }

  // from normal create
  public VeloxWritableColumnVector(int capacity, DataType dataType) {
    super(capacity, dataType);
    this.nativeColumnarVector = new NativeColumnarVector(dataType);
    if (!(dataType instanceof ArrayType || dataType instanceof MapType)) {
      nativeColumnarVector.reserve(capacity);
    }
    this.operationType = "WRITER";
    nativeColumnarVector.reserve(capacity);
    initInternal();
  }

  //for read, it's can't be a  dictionary vector
  public VeloxWritableColumnVector(NativeColumnarVector nativeColumnarVector, DataType dataType) {
    super(nativeColumnarVector.capacity(), dataType);
    this.nativeColumnarVector = nativeColumnarVector;
    this.operationType = "READ";
    bind();
  }

  void bind() {
    if (dataType().sameType(DataTypes.BooleanType)) {
      isBoolean = true;
    }
    if (isString()) {
      this.childColumns = new WritableColumnVector[1];
      this.childColumns[0] = this;
    } else if (isArray()) {
      DataType childType;
      childType = ((ArrayType) type).elementType();
      this.childColumns = new WritableColumnVector[1];
      this.childColumns[0] = bindVector(nativeColumnarVector.newChildWithIndex(0), childType);
    } else if (type instanceof StructType) {
      StructType st = (StructType) type;
      this.childColumns = new WritableColumnVector[st.fields().length];
      for (int i = 0; i < childColumns.length; ++i) {
        this.childColumns[i] = bindVector(nativeColumnarVector.newChildWithIndex(i), st.fields()[i].dataType());
      }
    } else if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      this.childColumns = new WritableColumnVector[2];
      this.childColumns[0] = bindVector(nativeColumnarVector.newChildWithIndex(0), mapType.keyType());
      this.childColumns[1] = bindVector(nativeColumnarVector.newChildWithIndex(1), mapType.valueType());
    } else if (type instanceof CalendarIntervalType) {
      throw new UnsupportedOperationException();
      // Three columns. Months as int. Days as Int. Microseconds as Long.
    } else {
      this.childColumns = null;
    }
    if ("DICTIONARY".equals(nativeColumnarVector.encoding())) {
      dictionaryVector = bindVector(nativeColumnarVector.valueVector(), dataType());
      dictionaryIds = bindVector(nativeColumnarVector.dictIdVector(), DataTypes.IntegerType);
      hasNull = nativeColumnarVector.mayHasNulls();
      if (hasNull) {
        this.nullAddress = nativeColumnarVector.dataAddress(NativeColumnarVector.DataTypeEnum.NULL);
      }
    } else {
      bindAddress();
    }
  }

  void initInternal() {
    if (dataType().sameType(DataTypes.BooleanType)) {
      isBoolean = true;
    }
    initChildren();
    bindAddress();

  }

  @Override
  protected boolean isArray() {
    return type instanceof ArrayType;
  }

  protected boolean isString() {
    return type instanceof BinaryType || type instanceof StringType;
  }

  void bindAddress() {
    if (isArray() || type instanceof MapType) {
      this.lengthAddress = nativeColumnarVector.dataAddress(NativeColumnarVector.DataTypeEnum.LENGTHS);
      this.offsetAddress = nativeColumnarVector.dataAddress(NativeColumnarVector.DataTypeEnum.OFFSETS);
    } else if (dataType() instanceof StructType || dataType() instanceof NullType) {
    } else {
      this.dataAddress = nativeColumnarVector.dataAddress(NativeColumnarVector.DataTypeEnum.DATA);
    }
    hasNull = nativeColumnarVector.mayHasNulls();
    if (hasNull) {
      this.nullAddress = nativeColumnarVector.dataAddress(NativeColumnarVector.DataTypeEnum.NULL);
    }
  }


  void initChildren() {
    if (isString()) {
      this.childColumns = new WritableColumnVector[1];
      this.childColumns[0] = this;
    } else if (isArray()) {
      DataType childType;
      int childCapacity = capacity;
      if (type instanceof ArrayType) {
        childType = ((ArrayType) type).elementType();
      } else {
        childType = DataTypes.ByteType;
        childCapacity *= DEFAULT_ARRAY_LENGTH;
      }
      this.childColumns = new WritableColumnVector[1];
      this.childColumns[0] = createVector(childCapacity, nativeColumnarVector.newChildWithIndex(0), childType);
    } else if (type instanceof StructType) {
      StructType st = (StructType) type;
      this.childColumns = new WritableColumnVector[st.fields().length];
      for (int i = 0; i < childColumns.length; ++i) {
        this.childColumns[i] = createVector(capacity, nativeColumnarVector.newChildWithIndex(i), st.fields()[i].dataType());
      }
    } else if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      this.childColumns = new WritableColumnVector[2];
      this.childColumns[0] = createVector(capacity, nativeColumnarVector.newChildWithIndex(0), mapType.keyType());
      this.childColumns[1] = createVector(capacity, nativeColumnarVector.newChildWithIndex(1), mapType.valueType());
    } else if (type instanceof CalendarIntervalType) {
      throw new UnsupportedOperationException();
      // Three columns. Months as int. Days as Int. Microseconds as Long.
    } else {
      this.childColumns = null;
    }
  }

  public NativeColumnarVector getNative() {
    return nativeColumnarVector;
  }


  @Override
  public int getDictId(int rowId) {
    return Platform.getInt(null, dataAddress + 4L * rowId);
  }

  @Override
  protected void reserveInternal(int capacity) {
    if (capacity == 0) {
      return;
    }
    nativeColumnarVector.reserve(capacity);
    bindAddress();
    this.capacity = capacity;
  }

  @Override
  public void putNotNull(int rowId) {
  }

  @Override
  public void putNull(int rowId) {
    if (!hasNull) {
      this.nullAddress = nativeColumnarVector.dataAddress(NativeColumnarVector.DataTypeEnum.NULL);
      hasNull = true;
    }
    numNulls += 1;
    int byteIndex = rowId / 8; // 计算rowId对应的字节位置
    int bitIndex = rowId % 8;  // 计算在该字节中的位位置
    byte old = Platform.getByte(null, nullAddress + byteIndex);
    Platform.putByte(null, nullAddress + byteIndex, (byte) (old & ~(1 << bitIndex)));
  }

  @Override
  public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      putNull(rowId + i);
    }
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    // default not null
  }

  @Override
  public int numNulls() {
    return numNulls;
  }

  @Override
  public boolean hasNull() {
    return hasNull;
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (!hasNull) {
      return false;
    }
    int byteIndex = rowId / 8; // 计算rowId对应的字节位置
    int bitIndex = rowId % 8;  // 计算在该字节中的位位置
    byte currentByte = Platform.getByte(null, nullAddress + byteIndex);

    return (currentByte & (1 << bitIndex)) == 0;
  }


  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    int byteIndex = rowId / 8; // 计算rowId对应的字节位置
    int bitIndex = rowId % 8;  // 计算在该字节中的位位置
    byte old = Platform.getByte(null, dataAddress + byteIndex);
    if (value) {
      Platform.putByte(null, dataAddress + byteIndex, (byte) (old | (1 << bitIndex)));
    } else {
      Platform.putByte(null, dataAddress + byteIndex, (byte) (old & ~(1 << bitIndex)));

    }
//    nativeColumnarVector.putBoolean(rowId, value);
  }

  native void putBoolean(long address, int rowId, boolean value);

  @Override   // todo optimize
  public void putBooleans(int rowId, int count, boolean value) {
    for (int i = 0; i < count; ++i) {
      putBoolean(rowId + i, value);
    }
  }


  @Override
  public void putBooleans(int rowId, int count, byte src, int srcIndex) {
    byte[] byte8 = new byte[8];
    assert ((srcIndex + count) <= 8);
    byte8[0] = (byte) (src & 1);
    byte8[1] = (byte) (src >>> 1 & 1);
    byte8[2] = (byte) (src >>> 2 & 1);
    byte8[3] = (byte) (src >>> 3 & 1);
    byte8[4] = (byte) (src >>> 4 & 1);
    byte8[5] = (byte) (src >>> 5 & 1);
    byte8[6] = (byte) (src >>> 6 & 1);
    byte8[7] = (byte) (src >>> 7 & 1);
    for (int i = 0; i < count; i++) {
      putBoolean(rowId + i, byte8[srcIndex + i] == 1);
    }

  }

  @Override
  public void putBooleans(int rowId, byte src) {
    putBooleans(rowId, 8, src, 0);
  }

  @Override
  public boolean getBoolean(int rowId) {
    if (dictionaryVector != null) {
      return dictionaryVector.getBoolean(dictionaryIds.getDictId(rowId));
    }
    int byteIndex = rowId / 8; // 计算rowId对应的字节位置
    int bitIndex = rowId % 8;  // 计算在该字节中的位位置
    byte currentByte = Platform.getByte(null, dataAddress + byteIndex);

    return (currentByte & (1 << bitIndex)) != 0;
//    return nativeColumnarVector.getBoolean(rowId);
  }

  native boolean getBoolean(long address, int rowId);


  @Override
  public boolean[] getBooleans(int rowId, int count) {
    assert (dictionary == null);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = getBoolean(rowId + i);
    }
    return array;
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    if (isBoolean) {
      putBoolean(rowId, value == 1);
    } else {
      Platform.putByte(null, dataAddress + rowId, value);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {

    for (int i = 0; i < count; ++i) {
      Platform.putByte(null, dataAddress + rowId + i, value);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, dataAddress + rowId, count);
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionaryVector != null) {
      return dictionaryVector.getByte(dictionaryIds.getDictId(rowId));
    }
    return Platform.getByte(null, dataAddress + rowId);
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    assert (dictionary == null);
    byte[] array = new byte[count];
    Platform.copyMemory(null, dataAddress + rowId, array, Platform.BYTE_ARRAY_OFFSET, count);
    return array;
  }


  //
  // APIs dealing with shorts
  //

  @Override
  public void putShort(int rowId, short value) {

    Platform.putShort(null, dataAddress + 2L * rowId, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {

    long offset = dataAddress + 2L * rowId;
    for (int i = 0; i < count; ++i, offset += 2) {
      Platform.putShort(null, offset, value);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.SHORT_ARRAY_OFFSET + srcIndex * 2L, null, dataAddress + 2L * rowId, count * 2L);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, dataAddress + rowId * 2L, count * 2L);
  }

  @Override
  public short getShort(int rowId) {
    if (dictionaryVector != null) {
      return (short) dictionaryVector.getShort(dictionaryIds.getDictId(rowId));
    }
    return Platform.getShort(null, dataAddress + 2L * rowId);
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    assert (dictionary == null);
    short[] array = new short[count];
    Platform.copyMemory(null, dataAddress + rowId * 2L, array, Platform.SHORT_ARRAY_OFFSET, count * 2L);
    return array;
  }

  //
  // APIs dealing with ints
  //

  @Override
  public void putInt(int rowId, int value) {

    Platform.putInt(null, dataAddress + 4L * rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    long offset = dataAddress + 4L * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putInt(null, offset, value);
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.INT_ARRAY_OFFSET + srcIndex * 4L, null, dataAddress + 4L * rowId, count * 4L);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, dataAddress + rowId * 4L, count * 4L);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET, null, dataAddress + 4L * rowId, count * 4L);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = dataAddress + 4L * rowId;
      for (int i = 0; i < count; ++i, offset += 4, srcOffset += 4) {
        Platform.putInt(null, offset, Integer.reverseBytes(Platform.getInt(src, srcOffset)));
      }
    }
  }

  @Override
  public int getInt(int rowId) {
    if (dictionaryVector != null) {
      return dictionaryVector.getInt(dictionaryIds.getDictId(rowId));
    }
    return Platform.getInt(null, dataAddress + 4L * rowId);
  }

  @Override
  public int[] getInts(int rowId, int count) {
    assert (dictionary == null);
    int[] array = new int[count];
    Platform.copyMemory(null, dataAddress + rowId * 4L, array, Platform.INT_ARRAY_OFFSET, count * 4L);
    return array;
  }

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(null, dataAddress + 8L * rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    for (int i = rowId; i < rowId + count; ++i) {
      putLong(i, value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.LONG_ARRAY_OFFSET + srcIndex * 8L,
        null, dataAddress + 8L * rowId, count * 8L);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex,
        null, dataAddress + rowId * 8L, count * 8L);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    if (!bigEndianPlatform) {
      Platform.copyMemory(src, srcIndex + Platform.BYTE_ARRAY_OFFSET, null, dataAddress + 8L * rowId, count * 8L);
    } else {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      long offset = dataAddress + 8L * rowId;
      for (int i = 0; i < count; ++i, offset += 8, srcOffset += 8) {
        Platform.putLong(null, offset, Long.reverseBytes(Platform.getLong(src, srcOffset)));
      }
    }
  }


  @Override
  public long[] getLongs(int rowId, int count) {
    assert (dictionary == null);
    long[] array = new long[count];
    Platform.copyMemory(null, dataAddress + rowId * 8L, array, Platform.LONG_ARRAY_OFFSET, count * 8L);
    return array;
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {

    Platform.putFloat(null, dataAddress + rowId * 4L, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {

    long offset = dataAddress + 4L * rowId;
    for (int i = 0; i < count; ++i, offset += 4) {
      Platform.putFloat(null, offset, value);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.FLOAT_ARRAY_OFFSET + srcIndex * 4L, null, dataAddress + 4L * rowId, count * 4L);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, dataAddress + rowId * 4L, count * 4L);
  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

    if (!bigEndianPlatform) {
      putFloats(rowId, count, src, srcIndex);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = dataAddress + 4L * rowId;
      for (int i = 0; i < count; ++i, offset += 4) {
        Platform.putFloat(null, offset, bb.getFloat(srcIndex + (4 * i)));
      }
    }
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionaryVector != null) {
      return dictionaryVector.getFloat(dictionaryIds.getDictId(rowId));
    }
    return Platform.getFloat(null, dataAddress + rowId * 4L);
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    assert (dictionary == null);
    float[] array = new float[count];
    Platform.copyMemory(null, dataAddress + rowId * 4L, array, Platform.FLOAT_ARRAY_OFFSET, count * 4L);
    return array;
  }


  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {

    Platform.putDouble(null, dataAddress + rowId * 8L, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {

    long offset = dataAddress + 8L * rowId;
    for (int i = 0; i < count; ++i, offset += 8) {
      Platform.putDouble(null, offset, value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.DOUBLE_ARRAY_OFFSET + srcIndex * 8L, null, dataAddress + 8L * rowId, count * 8L);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {

    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET + srcIndex, null, dataAddress + rowId * 8L, count * 8L);
  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

    if (!bigEndianPlatform) {
      putDoubles(rowId, count, src, srcIndex);
    } else {
      ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
      long offset = dataAddress + 8L * rowId;
      for (int i = 0; i < count; ++i, offset += 8) {
        Platform.putDouble(null, offset, bb.getDouble(srcIndex + (8 * i)));
      }
    }
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionaryVector != null) {
      return dictionaryVector.getDouble(dictionaryIds.getDictId(rowId));
    }
    return Platform.getDouble(null, dataAddress + rowId * 8L);
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    assert (dictionary == null);
    double[] array = new double[count];
    Platform.copyMemory(null, dataAddress + rowId * 8L, array, Platform.DOUBLE_ARRAY_OFFSET, count * 8L);
    return array;
  }

  //
  // APIs dealing with Arrays.
  //
  @Override
  public void putArray(int rowId, int offset, int length) {
    Platform.putInt(null, lengthAddress + 4L * rowId, length);
    Platform.putInt(null, offsetAddress + 4L * rowId, offset);
  }


  int stringCapacity = 0;
  long stringOffset = 0;
  long stringBufferAddress;

  int allocationSize = 1024 * 8 * 8;

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    reserve(elementsAppended + 1);
    elementsAppended++;
    Platform.putInt(null, dataAddress + 16L * rowId, length);
    if (length <= 12) {
      for (int i = 0; i < length; i++) {
        Platform.putByte(null, dataAddress + 16L * rowId + 4 + i, value[offset + i]);
      }
    } else {
      if (stringCapacity < length) {
        // to void big string
        int aquire = Math.max(allocationSize, length);
        stringBufferAddress = nativeColumnarVector.allocateStringData(aquire);
        stringCapacity = allocationSize;
        stringOffset = stringBufferAddress;
      }
      for (int i = 0; i < 4; i++) {
        Platform.putByte(null, dataAddress + 16L * rowId + 4 + i, value[offset + i]);
      }
      Platform.putLong(null, dataAddress + 16L * rowId + 8, stringOffset);
      Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET + offset, null, stringOffset, length);
      stringCapacity -= length;
      stringOffset += length;
    }
    return 0;
  }

  @Override
  public long getLong(int rowId) {
    if (dictionaryVector != null) {
      return dictionaryVector.getLong(dictionaryIds.getDictId(rowId));
    }
    return Platform.getLong(null, dataAddress + 8L * rowId);
  }


  @Override
  public int getArrayLength(int rowId) {
    return Platform.getInt(null, lengthAddress + 4L * rowId);
  }

  @Override
  public int getArrayOffset(int rowId) {
    return Platform.getInt(null, offsetAddress + 4L * rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    if (dictionaryVector != null) {
      return dictionaryVector.getUTF8String(dictionaryIds.getDictId(rowId));
    }
    int length = Platform.getInt(null, dataAddress + 16L * rowId);
    if (length <= 12) {
      return UTF8String.fromAddress(null, dataAddress + 16L * rowId + 4, length);
    } else {
      long address = Platform.getLong(null, dataAddress + 16L * rowId + 8);
      return UTF8String.fromAddress(null, address, length);
    }
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    throw new UnsupportedOperationException();

  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return getUTF8String(rowId).getBytes();
  }

  @Override
  public ByteBuffer getByteBuffer(int rowId, int count) {
    throw new UnsupportedOperationException();

  }

  @Override
  protected WritableColumnVector reserveNewColumn(int capacity, DataType type) {
    return null;
  }

  native long getOffsetAddress(long ptr);

  native long getSizeAddress(long ptr);

  native long getDictIdAddress(long ptr);

  native long getDictAddress(long ptr);

  native long getArrayDataObjectAddress(long ptr);


  private static final Field field;

  static {
    try {
      field = ParquetDictionary.class.getDeclaredField("dictionary");
      field.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    if (dictionary == null) {
      nativeColumnarVector.removeDictionaryVector();
      if (this.dictionaryVector != null) {
        dictionaryVector.close();
        dictionaryVector = null;
      }
      if (dictionaryIds != null) {
        dictionaryIds.close();
        dictionaryIds = null;
      }
    }
    if (dictionary instanceof ParquetDictionary) {
      try {
        org.apache.parquet.column.Dictionary parquetDict = (org.apache.parquet.column.Dictionary) field.get(dictionary);
        if (this.dictionaryVector != null && parquetDict.equals(field.get(this.dictionary))) {
          return;
        }
        if (dictionaryVector != null) {
          dictionaryVector.close();
          dictionaryVector = null;
        }
        int capacity = parquetDict.getMaxId() + 1;
        NativeColumnarVector nativeDictionaryVector = nativeColumnarVector.createDictionaryVector(capacity);
        dictionaryVector = createVector(capacity, nativeDictionaryVector, dataType());
        loadToVelox(dictionaryVector, dictionary, capacity);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    this.dictionary = dictionary;
    // decode 到一半  切page 这个时候不需要变 dict vector
  }

  native void deleteDict(long address);

  public int size() {
    return capacity;
  }

  native long createDictVector(long address, int capacity);


  private void loadToVelox(VeloxWritableColumnVector dictVector, Dictionary parquetDict, int maxId) {
    dictVector.reserveInternal(maxId);
    if (type instanceof StringType || type instanceof BinaryType) {
      for (int i = 0; i < maxId; i++) {
        byte[] value = parquetDict.decodeToBinary(i);
        dictVector.putByteArray(i, value, 0, value.length);
      }
    } else if (type instanceof ByteType) {
      for (int i = 0; i < maxId; i++) {
        dictVector.putByte(i, (byte) parquetDict.decodeToInt(i));
      }
    } else if (type instanceof ShortType) {
      for (int i = 0; i < maxId; i++) {
        dictVector.putShort(i, (short) parquetDict.decodeToInt(i));
      }
    } else if (type instanceof IntegerType || type instanceof DateType || DecimalType.is32BitDecimalType(type) || type instanceof YearMonthIntervalType) {
      for (int i = 0; i < maxId; i++) {
        dictVector.putInt(i, parquetDict.decodeToInt(i));
      }
    } else if (type instanceof LongType || type instanceof TimestampType || type instanceof TimestampNTZType || DecimalType.is64BitDecimalType(type) || type instanceof DayTimeIntervalType) {
      for (int i = 0; i < maxId; i++) {
        dictVector.putLong(i, parquetDict.decodeToLong(i));
      }
    } else if (type instanceof FloatType) {
      for (int i = 0; i < maxId; i++) {
        dictVector.putFloat(i, parquetDict.decodeToFloat(i));
      }
    } else if (type instanceof DoubleType) {
      for (int i = 0; i < maxId; i++) {
        dictVector.putDouble(i, parquetDict.decodeToDouble(i));
      }
    } else if (childColumns != null) {
      // Nothing to store.
    } else {
      throw new RuntimeException("Unhandled " + type);
    }

  }

  // todo optimize for skip resize current vector

  @Override
  public WritableColumnVector reserveDictionaryIds(int capacity) {
    dictionaryIds = new VeloxWritableColumnVector(capacity, nativeColumnarVector.reserveDictionaryIds(capacity), DataTypes.IntegerType);
    reserveInternal(capacity);
    return dictionaryIds;
  }

  @Override
  public void reset() {
    if (!isClosed) {
      this.lengthAddress = -1;
      this.offsetAddress = -1;
      this.dataAddress = -1;
      this.nullAddress = -1;
      this.hasNull = false;
      numNulls = 0;
      stringCapacity = 0;
      nullElement = 0;
      elementsAppended = 0;
      nativeColumnarVector.reset();
      bindAddress();
      this.capacity = nativeColumnarVector.capacity();
      if (dictionaryIds != null) {
        dictionaryIds.close();
      }
      dictionaryIds = null;
    } else {
      throw new RuntimeException("Error for reset with closed column");
    }
  }


  @Override
  public void close() {
    if (!isClosed) {
      isClosed = true;
      if (dictionaryIds != null) {
        dictionaryIds.close();
      }
      if (dictionaryVector != null) {
        dictionaryVector.close();
      }
      if (childColumns != null) {
        for (ColumnVector child : childColumns) {
          if (!child.equals(this)) {
            child.close();
          }
        }
      }
      nativeColumnarVector.close();
    }
  }
}
