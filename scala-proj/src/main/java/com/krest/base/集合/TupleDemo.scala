package com.krest.base.集合

// 元祖
object TupleDemo {
  def main(args: Array[String]): Unit = {
    val tuple = new Tuple1("hello")
    //    元祖中可以放入各式各样的元素，数字代表你可以放入多少元素，最多支持 22 个元素
    //    val tuple1: (Int, Int, String, Boolean) = Tuple4(1, 2, "String", true)
    //    这种也是元祖
    val tuple2: (Int, Int, Int, Int, Int) = (1, 2, 3, 4, 5)
    //    println(tuple2._3)

    // 元祖没有 for 和 forEach
    val iterator: Iterator[Any] = tuple2.productIterator
    iterator.foreach(println)
  }
}
