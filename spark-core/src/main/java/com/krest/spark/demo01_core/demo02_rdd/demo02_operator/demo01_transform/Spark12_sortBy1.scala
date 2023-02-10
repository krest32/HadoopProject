package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_sortBy1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // sortBy
    val rdd = sc.makeRDD(List(6, 2, 4, 5, 3, 1), 2)

    val newRDD: RDD[Int] = rdd.sortBy(num => num)

    newRDD.saveAsTextFile("output")


    sc.stop()

  }
}
