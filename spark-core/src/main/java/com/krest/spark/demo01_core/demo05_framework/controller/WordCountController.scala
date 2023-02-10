package com.krest.spark.demo01_core.demo05_framework.controller

import com.krest.spark.demo01_core.demo05_framework.common.TController
import com.krest.spark.demo01_core.demo05_framework.service.WordCountService


/**
  * 控制层
  */
class WordCountController extends TController {

    private val wordCountService = new WordCountService()

    // 调度
    def dispatch(): Unit = {
        // TODO 执行业务操作
        val array = wordCountService.dataAnalysis()
        array.foreach(println)
    }
}
