package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ForEach {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // Driver 端循环遍历方法：以分区进行采集，会保持顺序
    rdd.collect().foreach(println)

    println("++++++++++++++++++++++++++++++++")
    // 不会保持顺序
    // Executor端循环打印
    rdd.foreach(println)
    sc.stop()
  }
}
