/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.columnar.extension.vector;

import java.util.Iterator;
import org.apache.spark.sql.execution.columnar.VeloxColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ColumnarBatchInIterator implements AutoCloseable {
  public ColumnarBatchInIterator(Iterator<ColumnarBatch> delegated) {
    this.delegated = delegated;
  }

  ColumnarBatch currentBatch;   // ref to void gc

  public long next() {
    try {
      final ColumnarBatch batch = nextColumnarBatch();
      if (currentBatch != null) {
        currentBatch.close();
      }
      currentBatch = batch;
      return ((VeloxColumnarBatch) batch).nativeObje().nativePTR();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    if (currentBatch != null) {
      currentBatch.close();
    }
  }

  protected final Iterator<ColumnarBatch> delegated;
  private transient ColumnarBatch nextBatch = null;

  long totalTimes = 0;

  public boolean hasNext() {
    while (delegated.hasNext()) {
      long l = System.nanoTime();
      nextBatch = delegated.next();
      if (nextBatch.numRows() > 0) {
        totalTimes += System.nanoTime() - l;
        // any problem using delegated.hasNext() instead?
        return true;
      }
    }
    try {
      close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return false;
  }


  public ColumnarBatch nextColumnarBatch() {
    return nextBatch;
  }
}
