package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 内存中创建Rdd
 */
object BuildRdd01 {
  def main(args: Array[String]): Unit = {

    //    准备环境 * 表示本机的线程核数，如果没有【*】，则是单线程运行
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //    创建RDD
    val seq = Seq[Int](1, 2, 3, 4)
    // parallelize 并行处理的意思, makeRdd 与 parallelize 方法一致
    //    val rdd: RDD[Int] = sc.parallelize(seq)
    val rdd = sc.makeRDD(seq)
    rdd.collect().foreach(println)

    //    关闭环境
    sc.stop()
  }
}
