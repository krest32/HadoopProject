package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 简单使用, 需要返回一个可以迭代的集合
 */
object RddFlatMap02 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理
    val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))

    val flat: RDD[String] = rdd.flatMap(s => {
      s.split(" ")
    })

    flat.collect().foreach(println)

    sc.stop()

  }
}
