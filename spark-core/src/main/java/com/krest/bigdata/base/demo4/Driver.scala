package com.krest.bigdata.base.demo4

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 链接服务器
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val task = new Task()

    //   对象输出流
    val out1 = client1.getOutputStream
    val objectOut1 = new ObjectOutputStream(out1)

    val subTask1 = new SubTask
    subTask1.logic = task.logic
    subTask1.datas = task.datas.take(2)

    // 准备发送对象
    objectOut1.writeObject(subTask1)
    objectOut1.flush()
    objectOut1.close()
    client1.close()


    //   对象输出流
    val out2 = client2.getOutputStream
    val objectOut2 = new ObjectOutputStream(out2)

    val subTask2 = new SubTask
    subTask2.logic = task.logic
    subTask2.datas = task.datas.takeRight(2)

    // 准备发送对象
    objectOut2.writeObject(subTask2)
    objectOut2.flush()
    objectOut2.close()
    client2.close()
    println("客户端数据发送完毕")
  }
}
