package com.krest.base.方法与函数

/**
 * 隐式调用
 *
 * 被 implicit 修饰，可以直接被隐式调用
 * 仅允许有一个类型的隐式修饰
 */
object Demo5 {
  def sayName(implicit name: String): Unit = {
    println(s"$name is a student")
  }

  def main(args: Array[String]): Unit = {
    implicit val name: String = "krest"
    //    不能有两个String
    //    implicit val age: String = "12"
    sayName
    sayName("kara")
  }
}
