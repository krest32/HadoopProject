package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Save {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 3, 4), 2)
    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")

    sc.stop()
  }
}
