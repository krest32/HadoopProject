package com.krest.bigdata.base.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 未使用累加器之前的限制
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      1, 2, 3, 4
    ))

    // driver 进行初始化
    var sum = 0

    // executor 执行累加，但是执行累加之后， sum数据没有返回
    rdd.foreach(num => {
      sum += num
    })

    // 最终输出为 0，所以并不符合预期
    println(sum)

    sc.stop()
  }
}
