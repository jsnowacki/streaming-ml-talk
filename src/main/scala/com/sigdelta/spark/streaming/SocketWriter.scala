package com.sigdelta.spark.streaming

import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.ForeachWriter

import scala.util.{Success, Try}

class SocketWriter[T](host: String, port: Int) extends ForeachWriter[T] {

  protected var socket: Socket = _

  def open(partitionId: Long, version: Long): Boolean = {
    Try(new Socket(host, port)) match {
      case Success(s) =>
        socket = s
        true
      case _ => false
    }
  }

  def process(value: T): Unit = {
    val s = value.toString
    socket.getOutputStream.write(s"$s\n".getBytes(StandardCharsets.UTF_8))
  }

  def close(errorOrNull: Throwable): Unit = if (socket != null) socket.close()
}
