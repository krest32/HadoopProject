package com.krest.bigdata.framework.application

import com.krest.bigdata.framework.common.Tapplication
import com.krest.bigdata.framework.controller.WordCountController

object WordCountApplication extends App with Tapplication {

  //  启动应用程序
  start() {
    val controller = new WordCountController
    controller.execute
  }

}
