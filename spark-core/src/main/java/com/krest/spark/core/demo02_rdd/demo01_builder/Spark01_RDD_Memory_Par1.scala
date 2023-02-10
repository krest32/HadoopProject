package com.krest.spark.core.demo02_rdd.demo01_builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par1 {

  def main(args: Array[String]): Unit = {

    //  准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 创建RDD，并保存分区文件
    // 【1，2】，【3，4】
    val rdd1 = sc.makeRDD(List(1,2,3,4), 2)
    // 将处理的数据保存成分区文件
    rdd1.saveAsTextFile("output1")

    // 【1】，【2】，【3，4】
    val rdd2 = sc.makeRDD(List(1,2,3,4), 3)
    rdd2.saveAsTextFile("output2")

    // 【1】，【2,3】，【4,5】
    val rdd3 = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    rdd3.saveAsTextFile("output3")


    //关闭环境
    sc.stop()
  }
}
