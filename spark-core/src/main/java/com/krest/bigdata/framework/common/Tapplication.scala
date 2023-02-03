package com.krest.bigdata.framework.common

import com.krest.bigdata.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait Tapplication {


  /**
   * op -> 传入需要执行的代码
   * master app => 设定默认值
   *
   * @param op
   */
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {

    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    //关闭 Spark 连接
    sc.stop()
    EnvUtil.clear()
  }
}
