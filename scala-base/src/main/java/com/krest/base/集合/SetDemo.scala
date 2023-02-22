package com.krest.base.集合

/**
 * set 无序，自动去重
 */
object SetDemo {
  def main(args: Array[String]): Unit = {
    //    不可边长 Set
    //    val set = Set[Int](1, 2, 3, 4, 4, 5)
    //    set.foreach(println)
    //    println(set.size)

    //    取两个set的交集
    //    val set2 = Set[Int](1, 2, 3)
    //    val ints: Set[Int] = set.intersect(set2)
    //    ints.foreach(println)

    //    操作符号操作(取交集)
    //    val set2 = Set[Int](1, 2, 3)
    //    val ints: Set[Int] = set & set2
    //    ints.foreach(println)


    // 函数式表达式：过滤
    //    val ints: Set[Int] = set.filter(s => {
    //      s > 2
    //    })
    //    ints.foreach(println)


    //    可变长Set，追加元素
    import scala.collection.mutable.Set
    val set = Set[Int](1, 2, 3)
    val value: set.type = set.+=(4, 5, 6, 7)
    value.foreach(println)
  }
}
