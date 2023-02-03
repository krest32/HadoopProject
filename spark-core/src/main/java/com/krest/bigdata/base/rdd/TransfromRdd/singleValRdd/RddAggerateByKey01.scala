package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 理解起来稍微有些难理解
 * 可以将【分区内】和【分区外】的计算规则分离开
 */
object RddAggerateByKey01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理，但是key的数据如果只有一个，那么是不会进行计算的
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    /**
     * 1 表示为初始值，用于碰到第一个key的时候进行计算
     * 2.1 分区内的计算规则
     * 2.2 分区恩爱的计算规则
     */
    val aggregateRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    aggregateRdd.collect().foreach(println)
    sc.stop()
  }
}
