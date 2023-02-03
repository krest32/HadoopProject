package com.krest.bigdata.base.demo3

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 链接服务器
    val client = new Socket("localhost", 9999)

    val out = client.getOutputStream
    //   对象输出流
    val objectOut = new ObjectOutputStream(out)

    // 准备发送对象
    val task = new Task
    objectOut.writeObject(task)
    objectOut.flush()
    objectOut.close()
    client.close()

    println("客户端数据发送完毕")
  }
}
