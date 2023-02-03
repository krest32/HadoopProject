package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object count {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)
    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 统计 Rdd 中元素的个数
    val i = rdd.count()
    println(i)
    sc.stop()
  }
}
