package org.apache.spark.rpc.netty

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.internal.StarryConf.SHUFFLE_SERVER_PORT
import org.apache.spark.util.Utils

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import scala.util.control.NonFatal

object NettyUtil {
  def sendByteBuffer(
      rpc: RpcEnv,
      nettyRpcEndpointRef: NettyRpcEndpointRef,
      byteBuffer: ByteBuffer): Unit = {
    rpc
      .asInstanceOf[NettyRpcEnv]
      .send(new ByteBufferRequestMessage(rpc.address, nettyRpcEndpointRef, byteBuffer))
  }

  def getRemoteAddress(rpcEndpointRef: RpcEndpointRef): Option[RpcAddress] = {
    if (rpcEndpointRef.address != null) {
      return Option.apply(rpcEndpointRef.address)
    }
    rpcEndpointRef match {
      case nettyRpcEndpointRef: NettyRpcEndpointRef =>
        val value = nettyRpcEndpointRef.client.getSocketAddress.asInstanceOf[InetSocketAddress]
        Option.apply(RpcAddress(value.getHostString, value.getPort))
      case other =>
        Option.empty
    }
  }
  def startServer(): RpcAddress = {
    val nettyEnv = SparkEnv.get.rpcEnv.asInstanceOf[NettyRpcEnv]
    val port = SparkEnv.get.conf.get(SHUFFLE_SERVER_PORT)
    val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
      nettyEnv.startServer(Utils.localHostName(), actualPort)
      (nettyEnv, actualPort)
    }
    try {
      val finialPort = Utils.startServiceOnPort(port, startNettyRpcEnv, SparkEnv.get.conf, "shuffle manager")._2
      RpcAddress(Utils.localHostName(), finialPort)
    } catch {
      case NonFatal(e) =>
        nettyEnv.shutdown()
        throw e
    }
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
