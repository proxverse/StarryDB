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

package org.apache.spark.sql.shuffle

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.unsafe.types.UTF8String

sealed trait ToShuffleManagerMasterEndpoint
case class ShuffleStatics() extends ToShuffleManagerMasterEndpoint

case class RemoveExecutor(executorId: String) extends ToShuffleManagerMasterEndpoint

sealed trait ToShuffleManager

case class RegisterManagerManager(executorId: String, sender: RpcEndpointRef)
    extends ToShuffleManager

case class FetchShuffleStatics() extends ToShuffleManager

case class RemoveShuffle(shuffleId: Int) extends ToShuffleManager

case class AddBatch(shuffleId: Int, reduceId: Int, byte: UTF8String) extends ToShuffleManager

case class QueryBatch(shuffleId: Int, reduceId: Int) extends ToShuffleManager

case class FetchBatch(shuffleId: Int, reduceId: Int, number: Int) extends ToShuffleManager

sealed trait ShuffleManagerReplay

case class ShuffleStaticsReply(executorId: String, info: String) extends ShuffleManagerReplay

case class OK() extends ShuffleManagerReplay

case class BatchMessage(batch: UTF8String) extends ShuffleManagerReplay

case class Failed(reason: String) extends ShuffleManagerReplay
