package com.krest.bigdata.base.demo2

import java.net.ServerSocket

object Executor {
  def main(args: Array[String]): Unit = {
    //    启动服务器，接收数据
    val server = new ServerSocket(9999)

    //    等待客户端链接
    println("服务器启动，等到接受数据")
    val socket = server.accept()


    val stream = socket.getInputStream
    val values = stream.read()

    println("接受客户端数据：" + values)
    stream.close()
    server.close()
  }
}
