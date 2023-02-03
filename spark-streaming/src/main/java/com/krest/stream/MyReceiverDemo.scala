package com.krest.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/** *
 * 自定义数据采集器
 */
object MyReceiverDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    val ssc = new StreamingContext(conf, Seconds(2))

    val messgeStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    messgeStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 自定义数据采集器
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true

    override def onStart(): Unit = {
      new Thread(() => {
        while (flag) {
          val message = "采集的数据为:" + new Random().nextInt(10).toString
          store(message)
          Thread.sleep(500)
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}
