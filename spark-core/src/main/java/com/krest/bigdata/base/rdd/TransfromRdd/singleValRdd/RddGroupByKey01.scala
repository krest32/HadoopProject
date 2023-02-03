package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 会将相同 key 的数据分到一个组中
 */
object RddGroupByKey01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理，但是key的数据如果只有一个，那么是不会进行计算的
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    groupRdd.collect().foreach(println)
    sc.stop()
  }
}
