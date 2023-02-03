package com.krest.bigdata.base.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器，将 executor 计算之后的结果返回给 driver
 */
object Demo03 {
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

    /**
     * 获取累加器的值
     * 特殊情况：
     * 1. 少加，如果没有行动算子，则不会执行
     * 2. 多加，多次执行行动算子，或导致执行多次累加
     */
    println(sumAcc.value)

    sc.stop()
  }
}
