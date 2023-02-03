package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 扩大分区
 */
object RddSortBy01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 6, 5, 4), 2)
    val sortedRdd: RDD[Int] = rdd.sortBy(num => num)
    sortedRdd.saveAsTextFile("output")

    sc.stop()

  }
}
