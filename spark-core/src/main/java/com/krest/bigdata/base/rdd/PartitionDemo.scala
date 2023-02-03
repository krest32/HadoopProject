package com.krest.bigdata.base.rdd

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 自定义数据分区器
 */
object PartitionDemo {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxxxxxxxxx"),
      ("cba", "xxxxxxxxxxxxxxxx"),
      ("wba", "xxxxxxxxxxxxxxxx"),
      ("nba", "xxxxxxxxxxxxxxxx"),
    ), 3)

    val partitionRdd: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partitionRdd.saveAsTextFile("output")

    sc.stop()
  }


  /**
   * 自定义数据分区器
   */
  class MyPartitioner extends Partitioner {
    // 分区的数量
    override def numPartitions: Int = 3

    //  返回数据分区的索引，从 0 开始
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wba" => 1
        case _ => 2
      }
    }
  }

}
