
package org.apache.spark.sql.execution.columnar.cache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.columnar.CachedBatch;
import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.KnownSizeEstimation;
import org.apache.spark.util.TaskCompletionListener;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CachedVeloxBatch implements AutoCloseable, KnownSizeEstimation, Externalizable, KryoSerializable, CachedBatch {

  public VeloxColumnarBatch veloxBatch;
  private StructType schema;

  public CachedVeloxBatch(VeloxColumnarBatch veloxBatch, StructType schema) {
    this.veloxBatch = veloxBatch;
    this.schema = schema;
  }

  public CachedVeloxBatch() {
  }

  @Override
  public void close() throws IOException {
    veloxBatch.close();
  }

  @Override
  public int numRows() {
    return veloxBatch.numRows();
  }

  @Override
  public long sizeInBytes() {
    return 100;
  }

  @Override
  public long estimatedSize() {
    return 100; // unlimit current
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    String json = schema.json();
    writerBytes(out, json.getBytes());
    byte[] bytes = veloxBatch.serialize();
    writerBytes(out, bytes);
  }

  private static void writerBytes(ObjectOutput out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    schema = (StructType) DataType.fromJson(new String(readBytes(in)));
    veloxBatch = VeloxColumnarBatch.createFromJson(
        readBytes(in),
        schema);
    if (TaskContext.get() != null) {
      TaskContext.get().addTaskCompletionListener((TaskCompletionListener) a -> veloxBatch.close());
    }
  }

  @NotNull
  private static byte[] readBytes(ObjectInput in) throws IOException {
    int sizeInBytes = in.readInt();
    byte[] bytes = new byte[sizeInBytes];
    in.readFully(bytes);
    return bytes;
  }

  @Override
  public void write(Kryo kryo, Output out) {
    byte[] bytes = schema.json().getBytes();
    out.writeInt(bytes.length);
    out.write(bytes);
    bytes = veloxBatch.serialize();
    out.writeInt(bytes.length);
    out.write(bytes);


  }

  @Override
  public void read(Kryo kryo, Input in) {
    schema = (StructType) DataType.fromJson(new String(readBytes(in)));
    veloxBatch = VeloxColumnarBatch.createFromJson(
        readBytes(in),
        schema);
  }

  @NotNull
  private static byte[] readBytes(Input in) {
    int sizeInBytes = in.readInt();
    byte[] bytes = new byte[sizeInBytes];
    in.read(bytes);
    return bytes;
  }
}
