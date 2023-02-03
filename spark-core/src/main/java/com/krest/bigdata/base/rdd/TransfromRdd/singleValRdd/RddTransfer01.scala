package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.{SparkConf, SparkContext}

object RddTransfer01 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))



    // 转化方法
    def mapFunction(num: Int): Int = {
      num * 2
    }
    // 操作数据
    val mapRdd = rdd.map(mapFunction)

    // 打印
    mapRdd.collect().foreach(println)

    sc.stop()
  }
}
