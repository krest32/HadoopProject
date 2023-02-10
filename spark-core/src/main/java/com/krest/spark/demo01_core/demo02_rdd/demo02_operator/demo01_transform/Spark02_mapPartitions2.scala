package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_mapPartitions2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 【1，2】，【3，4】
    // 【2】，【4】
    val mpRDD = rdd.mapPartitions(
      iter => {
        // 返回每个分区的最大值
        List(iter.max).iterator
      }
    )
    mpRDD.collect().foreach(println)

    sc.stop()

  }
}
