package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 简单实操
 */
object RddTransferDemo01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    //    一行一行的拿取数据
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    // 截取数据，获取路径
    val mapRdd: RDD[String] = rdd.map(line => {
      val tempStrData = line.split(" ")(6)
      tempStrData
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
