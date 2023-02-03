package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 体现数据处理的并行
 */
object RddReduceByKey01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理，但是key的数据如果只有一个，那么是不会进行计算的
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    val reduceRdd: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      x + y
    })

    reduceRdd.collect().foreach(println)
    sc.stop()
  }
}
