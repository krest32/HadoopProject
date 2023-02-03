package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 体现数据处理的并行
 */
object RddTransferDemo02 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 只有一个分区，顺序执行
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4), 1)

    val mapRdd1 = rdd.map(num => {
      println("+++++" + num)
      num
    })

    val mapRdd2 = mapRdd1.map(num => {
      println("------" + num)
      num
    })

    mapRdd2.collect()
    sc.stop()
  }
}
