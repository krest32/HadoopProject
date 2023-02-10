package com.krest.spark.core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_map1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - map 多种使用方式
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 1,2,3,4
    // 2,4,6,8


    // 转换函数
    def mapFunction(num: Int): Int = {
      num * 2
    }

    // 方式一 使用转换函数
    val mapRDD1: RDD[Int] = rdd.map(mapFunction)
    mapRDD1.collect().foreach(println)
    println("-----------------")
    // 方式二 使用匿名函数
    val mapRDD2: RDD[Int] = rdd.map((num: Int) => {
      num * 2
    })
    mapRDD2.collect().foreach(println)
    println("-----------------")

    // 方式三 简化方式二
    val mapRDD3: RDD[Int] = rdd.map((num: Int) => num * 2)
    mapRDD3.collect().foreach(println)
    println("-----------------")

    // 方式四 简化方式三
    val mapRDD4: RDD[Int] = rdd.map((num) => num * 2)
    mapRDD4.collect().foreach(println)
    println("-----------------")

    // 方式五 简化方式四
    val mapRDD5: RDD[Int] = rdd.map(num => num * 2)
    mapRDD5.collect().foreach(println)
    println("-----------------")

    // 方案六 简化方案五
    val mapRDD6: RDD[Int] = rdd.map(_ * 2)
    mapRDD6.collect().foreach(println)

    sc.stop()

  }
}
