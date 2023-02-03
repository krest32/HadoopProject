package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // aggregate 会参与 分区内 与 分区之间 的计算
    // aggregateByKey 会参与 分区内 的计算
    val ans: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(ans)

    sc.stop()
  }
}
