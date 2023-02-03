package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.{SparkConf, SparkContext}

object RddTransfer02 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))


    // 转化方法
    //    原始代码
    //    val mapRdd = rdd.map(((num: Int) => {
    //      num * 2
    //    }))

    //    简化代码
    //    val mapRdd = rdd.map(num => num * 2)

    //    进一步简化
    val mapRdd = rdd.map(_ * 2)
    // 打印
    mapRdd.collect().foreach(println)

    sc.stop()
  }
}
