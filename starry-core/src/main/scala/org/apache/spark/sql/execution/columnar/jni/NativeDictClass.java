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
package org.apache.spark.sql.execution.columnar.jni;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.execution.dict.ExecutorDictManager;

public class NativeDictClass {

  public static long toDictVector(long id, int numblocks) {
    return ExecutorDictManager.fetchDictVectorAddress(id, numblocks).nativePTR();
  }

  @VisibleForTesting
  public static native long toDictVectorNative(long id, int numblocks);


  public static native long execute(long batchID, String query);

  public static native String tryCompile(String query);
}
