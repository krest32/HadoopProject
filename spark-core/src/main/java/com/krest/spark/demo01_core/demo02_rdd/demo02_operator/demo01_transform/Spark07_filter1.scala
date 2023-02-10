package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_filter1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //  filter
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 过滤奇数
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)

    filterRDD.collect().foreach(println)


    sc.stop()

  }
}
