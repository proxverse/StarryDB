package org.apache.spark.sql.execution.columnar;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ColumnBatchUtils {

  public static ColumnarBatch createWriterableColumnBatch(int capacity, StructType structType) {
    ColumnVector[] columnVectors = new ColumnVector[structType.size()];
    for (int i = 0; i < structType.size(); i++) {
      DataType dataType = structType.fields()[i].dataType();
      columnVectors[i] = VeloxWritableColumnVector.createVector(capacity, dataType);
    }
    VeloxColumnarBatch veloxColumnarBatch = new VeloxColumnarBatch(columnVectors);
    return veloxColumnarBatch;
  }

}
