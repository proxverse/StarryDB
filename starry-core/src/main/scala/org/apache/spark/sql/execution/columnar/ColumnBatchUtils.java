package org.apache.spark.sql.execution.columnar;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import static org.apache.spark.sql.execution.columnar.VeloxWritableColumnVector.createVector;

public class ColumnBatchUtils {

  public static final byte B_1 = 1;
  public static final byte B_2 = 1 << 1;
  public static final byte B_3 = 1 << 2;
  public static final byte B_4 = 1 << 3;
  public static final byte B_5 = 1 << 4;
  public static final byte B_6 = 1 << 5;
  public static final byte B_7 = 1 << 6;
  public static final byte B_8 = -127;

  public static final byte[] NULL_BITS = {B_1, B_2, B_3, B_4, B_5, B_6, B_7, B_8};


  public static ColumnarBatch createWriterableColumnBatch(int capacity, StructType structType) {
    ColumnVector[] columnVectors = new ColumnVector[structType.size()];
    for (int i = 0; i < structType.size(); i++) {
      DataType dataType = structType.fields()[i].dataType();
      columnVectors[i] = createVector(capacity, dataType);
    }
    VeloxColumnarBatch veloxColumnarBatch = new VeloxColumnarBatch(columnVectors);
    veloxColumnarBatch.setSchema(structType);
    return veloxColumnarBatch;
  }


  public static native byte getByteNative(long address, int index);

  public static native short getShortNative(long address, int index);

  public static native int getIntNative(long address, int index);

  public static native long getLongNative(long address, int index);

  public static native float getFloatNative(long address, int index);

  public static native double getDoubleNative(long address, int index);

  public static native void getTimestampNative(long address, int index);

  public static native long createVeloxColumnBatch(int capacity, long cSchema);

  public static native int getVectorSize(long cSchema);

  public static native long columnAt(long cSchema, int col);

  public static native int numRows(long cSchema);

  public static native long toRowPTR(long cSchema);

  public static native long estimateFlatSize(long address);


  public static native long deserialize(byte[] data, long cSchema);

  public static native byte[] serialize(long batchHandle);


  public static native String getBatchMemorySize();

//  public static VeloxColumnarBatch createBatchFromAddress(long batchHandle, StructType schema) {
//    int numRows = ColumnBatchUtils.numRows(batchHandle);
//    ReadableColumnVector[] writableVeloxColumnVectors = new ReadableColumnVector[schema.size()];
//    for (int i = 0; i < schema.size(); i++) {
//      long columnAddress = ColumnBatchUtils.columnAt(batchHandle, i);
//      if (ReadableVeloxColumnVector.isConstantEncoding(columnAddress)) {
//        writableVeloxColumnVectors[i] = new ConstantVeloxColumnVector(numRows, ColumnBatchUtils.columnAt(batchHandle, i), schema.apply(i).dataType());
//      } else {
//        writableVeloxColumnVectors[i] = new ReadableVeloxColumnVector(numRows, ColumnBatchUtils.columnAt(batchHandle, i), schema.apply(i).dataType());
//      }
//    }
//    VeloxColumnarBatch veloxColumnarBatch = new VeloxColumnarBatch(writableVeloxColumnVectors, batchHandle);
//    veloxColumnarBatch.setNumRows(numRows);
//    return veloxColumnarBatch;
//  }


}
