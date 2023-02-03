package com.krest.bigdata.base.rdd.actionRdd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object collect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)
    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 会将不同分数的数据按照分许顺序采集到Driver端内存中，形成数组
    val i = rdd.collect()
    println(i.mkString(","))
    sc.stop()
  }
}
