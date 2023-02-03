package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom 简单使用，将一个分区的数据变成一个数组
 */
object RddGlom01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val glomRdd: RDD[Array[Int]] = rdd.glom()

    glomRdd.collect().foreach(data => println(data.mkString(",")))

    sc.stop()
  }
}
