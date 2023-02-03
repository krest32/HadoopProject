package com.krest.bigdata.base.cases.need01

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 针对 demo03 问题
 * 程序中仍然有shuffle操作，所以性能上还是会有一点点慢，
 * 所以换一种思路，比如通过累加器的方式进行实现
 */
object Demo04 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hot top 10")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取日志数据
    val actionRdd: RDD[String] = sc.textFile("CoreCaseData")

    val acc = new HotCateGoryAcc
    sc.register(acc, "HotCateGoryAcc")


    actionRdd.foreach(line => {
      val data = line.split("_")
      if (data(6) != "-1") {
        acc.add(data(6), "click")
      } else if (data(8) != "null") {
        val cids = data(8).split(",")
        cids.foreach(cid => {
          acc.add(cid, "order")
        })
      } else if (data(10) != "null") {
        val cids = data(10).split(",")
        cids.foreach(cid => {
          acc.add(cid, "pay")
        })
      }
    })

    val accVal: mutable.Map[String, HotCateGory] = acc.value
    val categories: mutable.Iterable[HotCateGory] = accVal.map(_._2)

    // 降序排序，取前十
    val sortedData = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.pcayCnt > right.pcayCnt
          } else {
            false
          }
        } else {
          false
        }
      })


    // 将结果进行打印
    sortedData.take(10).foreach(println)


    sc.stop()
  }

  /**
   * 自定义累加器
   * In (品类，操作类型)
   * out: mutable.map[String,HotCategory]
   */
  class HotCateGoryAcc extends AccumulatorV2[(String, String), mutable.Map[String, HotCateGory]] {
    private val hcMap = mutable.Map[String, HotCateGory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCateGory]] = {
      new HotCateGoryAcc
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val unit = hcMap.getOrElse(cid, HotCateGory(cid, 0, 0, 0))
      if (actionType == "click") {
        unit.clickCnt += 1
      } else if (actionType == "order") {
        unit.orderCnt += 1
      } else if (actionType == "pay") {
        unit.pcayCnt += 1
      }
      hcMap.update(cid, unit)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCateGory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value
      map2.foreach {
        case (cid, hc) => {
          val unit = map1.getOrElse(cid, HotCateGory(cid, 0, 0, 0))
          unit.clickCnt += hc.clickCnt
          unit.orderCnt += hc.orderCnt
          unit.pcayCnt += hc.pcayCnt
          map1.update(cid, unit)
        }
      }
    }

    override def value: mutable.Map[String, HotCateGory] = {
      hcMap
    }
  }


  case class HotCateGory(cid: String, var clickCnt: Int, var orderCnt: Int, var pcayCnt: Int)

}
