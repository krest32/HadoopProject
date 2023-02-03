package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 左外链接
 */
object RddJoin02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理，但是key的数据如果只有一个，那么是不会进行计算的
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3), ("d", 2)
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5)
    ))
    val joinRdd = rdd1.leftOuterJoin(rdd2)
    joinRdd.collect().foreach(println)
  }

}
