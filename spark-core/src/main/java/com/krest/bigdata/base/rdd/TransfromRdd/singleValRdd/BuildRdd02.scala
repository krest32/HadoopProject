package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 文件中创建 Rdd
 */
object BuildRdd02 {
  def main(args: Array[String]): Unit = {

    //    准备环境 * 表示本机的线程核数，如果没有【*】，则是单线程运行
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //    创建RDD, 可以使用通配符
    //    还可以读取远程路径 "hdfs://hadoop100:8020/test/1.txt"
    //    val fileRDD: RDD[String] = sc.textFile("input/word1*.txt")
    //    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    //    wordRDD.collect().foreach(println)

    //    该方法以文件为单位读取数据，读取的结果显示为元祖
    val rdd = sc.wholeTextFiles("input")
    rdd.collect().foreach(println)

    //    关闭环境
    sc.stop()
  }
}
