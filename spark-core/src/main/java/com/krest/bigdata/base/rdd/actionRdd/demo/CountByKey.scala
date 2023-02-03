package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 3, 4), 2)
    // 根据 value 统计出现次数
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()

    println(intToLong)

    sc.stop()
  }
}
