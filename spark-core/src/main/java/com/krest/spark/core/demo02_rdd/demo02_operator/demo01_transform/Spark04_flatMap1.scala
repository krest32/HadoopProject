package com.krest.spark.core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_flatMap1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // flatmap
    val rdd: RDD[List[Int]] = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ))


    val flatRDD: RDD[Int] = rdd.flatMap(
      list => {
        list
      }
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
