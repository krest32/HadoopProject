package com.krest.base.集合

object MapDemo {
  def main(args: Array[String]): Unit = {
    //  默认不可变集合
    //    val map = Map[String, Int]("a" -> 100, "b" -> 200, ("c", 300), ("d", 400))
    //  普通遍历
    //    map.foreach(kv => {
    //      println(kv._1 + " " + kv._2)
    //    })

    // 获取某个元素
    //    val maybeInt: Option[Int] = map.get("a")
    //    println(maybeInt.get)

    // 携带默认的数值
    //    val value: Int = map.get("bb").getOrElse(1000)
    //    println(value)


    //    遍历 key 或者 值
    //    val keys: Iterable[String] = map.keys
    //    keys.foreach(println)
    //    keys.foreach(key => {
    //      println(map.get(key).get)
    //    })

    // 将 map1 丢到 mqp 当中
    //    val map1 = Map[String, Int]("a" -> 200, "b" -> 400, ("c", 800), ("d", 400))
    //    val newMap: Map[String, Int] = map.++(map1)
    //    newMap.foreach(kv => {
    //      println("key:" + kv._1 + "  val:" + kv._2)
    //    })

    import scala.collection.mutable.Map
    val map1 = Map[String, Int](("a", 100), ("b", 200))
    val map2 = Map[String, Int](("b", 400), ("c", 500))
    map1.put("d", 1000)
    map1.foreach(kv => {
      println(kv._1 + " : " + kv._2)
    })
  }
}
