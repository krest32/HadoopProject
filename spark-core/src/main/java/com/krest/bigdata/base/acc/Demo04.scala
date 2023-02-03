package com.krest.bigdata.base.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 自定义累加器，实现 wordCount
 */
object Demo04 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      "hello", "spark", "hello"
    ))


    // 自定义累加器
    val wcAdd = new MyAcc
    // 向Spark注册
    sc.register(wcAdd)
    rdd.foreach(
      word => {
        wcAdd.add(word)
      }
    )

    println(wcAdd.value)

    sc.stop()
  }

  /**
   * 需要继承 AccumulatorV2
   * IN: 累加器输入的数据类型 String
   * Out：累加器返回的数据类型 mutable， Map[String,Long]
   * 重写方法
   *
   */
  class MyAcc extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    // 累加器是否为空
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    // 复制
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAcc
    }

    // 重置：清空
    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(word: String): Unit = {
      val newCNnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCNnt)
    }

    // driver 合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newCnt = map1.getOrElse(word, 0L) + count
          map1.update(word, newCnt)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}