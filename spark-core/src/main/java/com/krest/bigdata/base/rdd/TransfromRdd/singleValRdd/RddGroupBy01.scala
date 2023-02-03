package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupby 根据数据进行分组
 */
object RddGroupBy01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    // 分组判断方法
    def groupFunc(num: Int): Int = {
      num % 2
    }

    val groupRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunc)

    groupRdd.collect().foreach(println)

    sc.stop()
  }
}
