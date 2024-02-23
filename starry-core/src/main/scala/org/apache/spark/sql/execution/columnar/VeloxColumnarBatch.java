package org.apache.spark.sql.execution.columnar;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.columnar.jni.NativeColumnarBatch;
import org.apache.spark.sql.execution.columnar.jni.NativeColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarBatchRow;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class VeloxColumnarBatch extends ColumnarBatch {
  private Logger LOG = LoggerFactory.getLogger(VeloxColumnarBatch.class);

  boolean isClosed = false;

  NativeColumnarBatch nativeColumnarBatch;
  private StructType schema;


  public static VeloxColumnarBatch createFromRowVectorHandle(long handle, List<Attribute> attributes) {
    NativeColumnVector rootVector = new NativeColumnVector(handle);
    ColumnVector[] columnVectors = new ColumnVector[attributes.size()];
    for (int i = 0; i < attributes.size(); i++) {
      columnVectors[i] = VeloxWritableColumnVector.bindVector(rootVector.newChildWithIndex(i), attributes.get(i).dataType());
    }

    VeloxColumnarBatch veloxColumnarBatch = new VeloxColumnarBatch(columnVectors, rootVector.capacity());
    rootVector.close();
    return veloxColumnarBatch;
  }

  public static VeloxColumnarBatch createFromJson(byte[] json, StructType structType) {
    VeloxColumnarBatch fromRowVector = createFromRowVector(NativeColumnVector.deserialize(json), structType);
    fromRowVector.setSchema(structType);
    return fromRowVector;
  }

  public static VeloxColumnarBatch createFromRowVector(NativeColumnVector rootVector, StructType structType) {
    ColumnVector[] columnVectors = new ColumnVector[structType.size()];
    for (int i = 0; i < structType.size(); i++) {
      columnVectors[i] = VeloxWritableColumnVector.bindVector(rootVector.newChildWithIndex(i), structType.fields()[i].dataType());
    }
    int capacity = rootVector.capacity();
    VeloxColumnarBatch veloxColumnarBatch = new VeloxColumnarBatch(columnVectors, capacity);
    rootVector.close();
    return veloxColumnarBatch;
  }


  public VeloxColumnarBatch(ColumnVector[] columns) {
    this(columns, 0);
  }


  public VeloxColumnarBatch(ColumnVector[] columns, int numRows) {
    super(columns, numRows);
    count.incrementAndGet();
    NativeColumnVector[] columnVectorAddrs = new NativeColumnVector[columns.length];
    for (int i = 0; i < columns.length; i++) {
      ColumnVector cv = columns[i];
      if (cv instanceof VeloxWritableColumnVector) {
        columnVectorAddrs[i] = ((VeloxWritableColumnVector) cv).getNative();
      } else if (cv instanceof WrappedColumnarVector) {
        columnVectorAddrs[i] = ((VeloxWritableColumnVector) ((WrappedColumnarVector) cv).getWrapped()).getNative();
      } else {
        throw new UnsupportedOperationException("VeloxColumnarBatch only support VeloxColumnVector");
      }
    }
    nativeColumnarBatch = new NativeColumnarBatch(columnVectorAddrs, numRows);
  }

  public ColumnVector[] getColumns() {
    return columns;
  }


  public void setSchema(StructType type) {
    if (type.catalogString().matches(".*[ ,]+.*")) {
      return;
    }
    this.schema = type;
    try {
      nativeColumnarBatch.setSchema(type);
    } catch (Exception e) {
      LOG.error("Error set schema : " + type.catalogString(), e);
    }
  }

  @Override
  public void setNumRows(int numRows) {
    this.numRows = numRows;
    nativeColumnarBatch.setNumRows(numRows);
  }


  boolean autoClose = true;

  public void setAutoClose() {      // from reader batch unable to close
    autoClose = true;
  }

  public void disableAutoClose() {      // from reader batch unable to close
    autoClose = true;
  }

  public static AtomicInteger count = new AtomicInteger();

  @Override
  public Iterator<InternalRow> rowIterator() {
    final int maxRows = numRows;
    final ColumnarBatchRow row = new ColumnarBatchRow(columns);
    return new Iterator<InternalRow>() {
      int rowId = 0;
      int i = init();

      int init() {
        if (TaskContext.get() != null) {
          TaskContext.get().addTaskCompletionListener(new TaskCompletionListener() {
            @Override
            public void onTaskCompletion(TaskContext context) {
              if (autoClose) {
                close();
              }
            }
          });
        }
        return 0;
      }


      @Override
      public boolean hasNext() {
        boolean b = rowId < maxRows;
        if (!b) {
          if (autoClose) {
            close();
          }
        }
        return b;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.rowId = rowId++;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public boolean isClosed() {
    return isClosed;
  }

  public NativeColumnarBatch nativeObject() {
    return nativeColumnarBatch;
  }


  public NativeColumnVector rowVector() {
    return nativeColumnarBatch.rowVector();
  }

  @Override
  public void close() {
    if (!isClosed) {
      count.decrementAndGet();
      isClosed = true;
      super.close();
      nativeColumnarBatch.close();
    }
  }

}
