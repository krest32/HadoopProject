package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object takeOrdered {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)
    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 5, 3, 4))
    // 表示排序后取几个元素
    val i = rdd.takeOrdered(3)
    println(i.mkString(","))
    sc.stop()
  }
}
