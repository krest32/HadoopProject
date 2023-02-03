package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 扩大分区
 */
object RddSortBy02 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    val sortedRdd = rdd.sortBy(t => t._1)
    sortedRdd.saveAsTextFile("output")

    sc.stop()

  }
}
