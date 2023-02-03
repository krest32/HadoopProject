package com.krest.bigdata.framework.controller


import com.krest.bigdata.framework.common.TController
import com.krest.bigdata.framework.service.WordCountService

class WordCountController extends TController{
  private val service = new WordCountService

  // 调度
  def execute(): Unit = {
    val array: Array[(String, Int)] = service.dataAnalysis()
    array.foreach(println)
  }
}
