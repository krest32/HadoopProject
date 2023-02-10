package com.krest.spark.core.demo02_rdd.demo01_builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 从文件中创建RDD，将文件中的数据作为处理的数据源
    // path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
    // 绝对路径
    val rdd1 = sc.textFile("D:\\mineworkspace\\idea\\classes\\atguigu-classes\\datas\\1.txt")
    rdd1.collect().foreach(println)

    // 相对路径
    val rdd2: RDD[String] = sc.textFile("datas/1.txt")
    rdd2.collect().foreach(println)

    // path路径可以是文件的具体路径，也可以目录名称
    val rdd3 = sc.textFile("datas")
    rdd3.collect().foreach(println)

    // path路径还可以使用通配符 *
    val rdd4 = sc.textFile("datas/1*.txt")
    rdd4.collect().foreach(println)


    // path还可以是分布式存储系统路径：HDFS
    val rdd5 = sc.textFile("hdfs://linux1:8020/test.txt")
    rdd5.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
