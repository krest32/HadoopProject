package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.{SparkConf, SparkContext}

object RddByKey01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理，但是key的数据如果只有一个，那么是不会进行计算的
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    /**
     * 全部都是 wordCount
     * 四个方法最终都走了相同的一个方法 ： combineByKeyWithClassTag
     */
    rdd.reduceByKey(_ + _)
    rdd.aggregateByKey(0)(_ + _, _ + _)
    rdd.foldByKey(0)(_ + _)
    rdd.combineByKey(v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y)

  }
}
