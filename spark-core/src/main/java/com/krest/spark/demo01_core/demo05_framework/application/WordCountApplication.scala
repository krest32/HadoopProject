package com.krest.spark.demo01_core.demo05_framework.application

import com.krest.spark.demo01_core.demo05_framework.common.TApplication
import com.krest.spark.demo01_core.demo05_framework.controller.WordCountController


object WordCountApplication extends App with TApplication{

    // 启动应用程序
    start(){
        val controller = new WordCountController()
        controller.dispatch()
    }

}
