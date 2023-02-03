package com.krest.bigdata.base.rdd.TransfromRdd.doubleValRdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 双数值需要保持数据类型一致
 */
object Demo01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 双 val 类型
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(5, 6, 3, 4))

    //    交集
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    //    并集
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    //    差集
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))


    sc.stop()

  }
}
