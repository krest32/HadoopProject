package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object fold {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // fold 会参与 分区内 与 分区之间 的计算，两者的计算逻辑一致是，简化了 aggregate 操作
    val ans: Int = rdd.fold(10)(_ + _)
    println(ans)

    sc.stop()
  }
}
