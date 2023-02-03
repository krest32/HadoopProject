package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 缩减分区,默认情况下，不会对已经分区的数据进行打乱并进行重新组合，
 * 但是这个方法可能会造成数据倾斜
 */
object RddCoalesce01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)
    // 合并缩减分区
    val coalesceRdd: RDD[Int] = rdd.coalesce(2)
    coalesceRdd.saveAsTextFile("CoalesceOutPut")

    sc.stop()

  }
}
