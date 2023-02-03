package com.krest.bigdata.framework.service

import com.krest.bigdata.framework.common.TService
import com.krest.bigdata.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService {

  private val dao = new WordCountDao

  def dataAnalysis(): Array[(String, Int)] = {
    val fileRDD: RDD[String] = dao.readFile("input")
    // 将文件中的数据进行分词,保存在一个list当中
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    // 转换数据结构 word => (word, 1)
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)
    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()
    // 打印结果
    word2Count
  }

}
