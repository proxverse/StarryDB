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

package org.apache.spark.rpc

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerMessages.ToBlockManagerMaster

import java.util

sealed trait ToMemorykManagerMasterEndpoint
case class MemoryStatics() extends ToMemorykManagerMasterEndpoint

case class RemoveExecutor(executorId: String) extends ToMemorykManagerMasterEndpoint

sealed trait ToMemoryManagerMaster

case class RegisterManagerManager(executorId: String, sender: RpcEndpointRef)
    extends ToMemoryManagerMaster

case class FetchMemoryStatics() extends ToMemoryManagerMaster



sealed trait ToMemoryManagerMasterReplay
case class MemoryStaticsReply(executorId: String, info: String) extends ToMemoryManagerMasterReplay