package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sample : 取样方法
 */
object RddSample01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    /**
     * 抽取数据 3个参数：
     * 1. 抽取的数据是否放回
     * 2. 每条数据被抽取的概率 ()
     * 3. 抽取数据时，随机算法的种子
     */

    rdd.sample(false,
      0.4,
      1)
      .collect()
      .foreach(println)

    sc.stop()

  }
}
