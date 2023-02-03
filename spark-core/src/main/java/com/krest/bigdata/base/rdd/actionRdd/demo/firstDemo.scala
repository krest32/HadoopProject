package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object firstDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)
    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 取数据源的第一个
    val i = rdd.first()
    println(i)
    sc.stop()
  }
}
