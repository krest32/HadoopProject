package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_map2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //  算子 - map

    // 1. rdd 的计算一个分区内的数据是一个一个执行逻辑
    //    只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。
    //    分区内数据的执行是有序的。
    // 2. 不同分区数据计算是无序的。
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD = rdd.map(
      num => {
        println(">>>>>>>> " + num)
        num
      }
    )

    val mapRDD1 = mapRDD.map(
      num => {
        println("######" + num)
        num
      }
    )

    mapRDD1.collect()

    sc.stop()

  }
}
