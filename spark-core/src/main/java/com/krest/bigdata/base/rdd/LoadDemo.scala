package com.krest.bigdata.base.rdd

import org.apache.spark.{SparkConf, SparkContext}

object LoadDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("output1")
    println(rdd1.collect().mkString(","))

    println("++++++++++++++++++++++++++")
    val rdd2 = sc.objectFile[(String, Int)]("output2")
    println(rdd2.collect().mkString(","))

    println("++++++++++++++++++++++++++")
    val rdd3 = sc.sequenceFile[String, Int]("output3")
    println(rdd3.collect().mkString(","))

    sc.stop()
  }
}
