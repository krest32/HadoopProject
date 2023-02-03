package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 简单使用, 需要返回一个可以迭代的集合
 */
object RddFlatMap03 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理 注意 3 不是一个集合
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(3, 4)))

    // 使用模式匹配
    val flat = rdd.flatMap(data => {
      data match {
        case list: List[_] => list
        case temp => List(temp)
      }
    })


    flat.collect().foreach(println)

    sc.stop()

  }
}
