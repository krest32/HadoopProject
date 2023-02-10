package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_map4 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - map
    val rdd = sc.textFile("datas/apache.log")

    // 截取字符串中的某列字符串
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()

  }
}
