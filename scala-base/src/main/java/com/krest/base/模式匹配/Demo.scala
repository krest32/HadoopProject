package com.krest.base.模式匹配

/**
 * 模式匹配
 */
object Demo {
  def main(args: Array[String]): Unit = {
    val t1 = (1, 2.22, "aa", true)
    val iterator: Iterator[Any] = t1.productIterator
    iterator.foreach(value => {
      MatchTest(value)
    })
  }

  //  match 使用
  def MatchTest(o: Any): Unit = {
    o match {
      case 1 => println("等于 1")
      case v: Int => println("type is Int, val is " + v)
      case d: Double => println("type is Double, val is " + d)
      //  下划线代表什么都没有匹配上,需要放在最后
      case _ => println("no match")
    }
  }


}
