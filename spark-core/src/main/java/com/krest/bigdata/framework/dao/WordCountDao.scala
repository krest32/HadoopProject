package com.krest.bigdata.framework.dao

import com.krest.bigdata.framework.common.TDao
import com.krest.bigdata.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{

  def readFile(path: String): RDD[String] = {
    // 读取文件数据
    EnvUtil.take().textFile("input")
  }
}
