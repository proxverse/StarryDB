package org.apache.spark.rpc.netty

import org.apache.spark.rpc.{RpcAddress, RpcEnv}

import java.nio.ByteBuffer

object NettyUtil {
  def sendByteBuffer(
      rpc: RpcEnv,
      nettyRpcEndpointRef: NettyRpcEndpointRef,
      byteBuffer: ByteBuffer): Unit = {
    rpc
      .asInstanceOf[NettyRpcEnv]
      .send(new ByteBufferRequestMessage(rpc.address, nettyRpcEndpointRef, byteBuffer))
  }
}

class ByteBufferRequestMessage(
    override val senderAddress: RpcAddress,
    override val receiver: NettyRpcEndpointRef,
    override val content: ByteBuffer)
    extends RequestMessage(senderAddress, receiver, content) {
  override def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    content
  }
}
