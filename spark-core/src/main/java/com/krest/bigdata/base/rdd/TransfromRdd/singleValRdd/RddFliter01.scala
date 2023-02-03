package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 简单使用, 需要返回一个可以迭代的集合
 * 数据分区后在进行过滤，但是锅炉后，分区内的数据可能不均喝，在生产环境下，可能会发生数据倾斜
 */
object RddFliter01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处 理
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)

    filterRDD.collect().foreach(println)
    sc.stop()

  }
}
