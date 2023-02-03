package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupby 根据数据进行分组，注意分组和分区没有必然的关系
 */
object RddGroupBy02 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理
    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hadoop", "scala"), 2)


    // 分组判断 : 相同的首字母放在一起
    val groupRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRdd.collect().foreach(println)

    sc.stop()
  }
}
