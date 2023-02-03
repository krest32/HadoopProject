package com.krest.bigdata.base.rdd.TransfromRdd.doubleValRdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 双数值需要保持数据类型一致
 */
object Demo02 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 双 val 类型
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // 分区数量不相等 或者 区域内的数字个数不一致，会报错
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = sc.makeRDD(List(5, 6, 3, 4), 2)


    // 拉链
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))


    sc.stop()

  }
}
