package com.krest.base.模式匹配

/**
 * 偏函数,类似于简化版的match
 * PartialFunction[A,B] A是传入的类型，B是返回的类型
 */
object Demo2 {
  def myTest: PartialFunction[String, Int] = {
    case "abc" => 2
    case _ => -1
  }

  def main(args: Array[String]): Unit = {
    println(myTest("abc"))
    println(myTest("abcd"))
  }


}
