package com.krest.spark.core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_glom1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // List => Int， Int => Array
    // 将数据转换为 数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString(",")))

    sc.stop()

  }
}
