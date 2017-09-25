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

package com.sigdelta.spark.streaming

import java.io._
import java.net.{ServerSocket, Socket, SocketException}
import java.nio.charset.StandardCharsets

import scala.math._

object KMeansSocketServer {
  def main(args: Array[String]): Unit = {

    val doWork = true
    val serverPort = 9999
    val server = new ServerSocket(serverPort)

    println(s"Server started on port $serverPort")

    while (doWork) {
      val socket = server.accept()
      val thread = KMeansSocketServerThread(socket)
      println(s"Client $socket started")

      thread.start()
    }
    server.close()
  }
}

case class KMeansSocketServerThread(socket: Socket) extends Thread("KMeansSocketServerThread") {

  override def run(): Unit = {
    val rand = scala.util.Random
    val r = 2.5
    var t = 0.0

    try {
      println(s"Thread ${this.getId}: server started")

      val out = new BufferedOutputStream(socket.getOutputStream)
      while (true) {
        Seq(t - 3 / 4.0 * Pi, t, t + 3 / 4.0 * Pi).foreach(q => {
          val x = r * sin(q) + rand.nextGaussian() / 3
          val y = r * cos(q) + rand.nextGaussian() / 3
          val line = s"""{"x": $x, "y": $y}""" + "\n"
          out.write(line.getBytes(StandardCharsets.UTF_8))
        })
        t += 0.02
        out.flush()
        Thread.sleep(200)
      }

      out.close()
      socket.close()
    }
    catch {
      case _: SocketException =>
        println(s"Thread ${this.getId}: server stopped") // avoid stack trace when stopping a client with Ctrl-C
      case e: IOException =>
        e.printStackTrace();
    }
  }
}

