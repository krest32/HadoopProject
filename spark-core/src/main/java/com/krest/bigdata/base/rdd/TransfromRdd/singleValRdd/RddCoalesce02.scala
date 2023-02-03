package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 扩大分区
 */
object RddCoalesce02 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 方法一 ： 第二个参数 判断是否将数据进行打乱重新组合
    //    val coalesceRdd: RDD[Int] = rdd.coalesce(3, shuffle = true)
    //    方法二 底层仍然使用 shuffle
    val coalesceRdd: RDD[Int] = rdd.repartition(3)
    coalesceRdd.saveAsTextFile("CoalesceOutPut")

    sc.stop()

  }
}
