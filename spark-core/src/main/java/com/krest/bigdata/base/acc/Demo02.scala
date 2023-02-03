package com.krest.bigdata.base.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器，将 executor 计算之后的结果返回给 driver
 */
object Demo02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      1, 2, 3, 4
    ))

    // 有多种数据类型 double collection 等
    val sumAcc = sc.longAccumulator("sum")
    rdd.foreach(num => {
      sumAcc.add(num)
    })


    println(sumAcc.value)

    sc.stop()
  }
}
