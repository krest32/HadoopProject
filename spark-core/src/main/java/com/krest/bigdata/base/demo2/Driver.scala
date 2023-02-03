package com.krest.bigdata.base.demo2

import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 链接服务器
    val client = new Socket("localhost", 9999)
    val outputStream = client.getOutputStream

    outputStream.write(2)

    outputStream.flush()
    outputStream.close()
    client.close()
  }
}
