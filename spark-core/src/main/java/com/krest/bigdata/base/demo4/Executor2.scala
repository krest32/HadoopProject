package com.krest.bigdata.base.demo4

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * 分布式计算小练习
 */
object Executor2 {
  def main(args: Array[String]): Unit = {
    //    启动服务器，接收数据
    val server = new ServerSocket(9999)


    //    等待客户端链接
    println("服务器启动:9999，等到接受数据")
    val socket = server.accept()


    val in = socket.getInputStream
    val objectInput = new ObjectInputStream(in)
    //    读取数据类型，并且转化数据类型为 Task
    val value: SubTask = objectInput.readObject().asInstanceOf[SubTask]

    //    接受到数据，直接开始计算
    val ints: List[Int] = value.compute()
    println("接受客户端数据,并计算结果为：" + ints)
    objectInput.close()
    server.close()
  }
}
