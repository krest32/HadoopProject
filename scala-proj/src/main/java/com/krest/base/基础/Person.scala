package com.krest.base.基础

class Person(xName: String, xAge: Int) {
  // 定义属性
  val name = xName
  val age = xAge

}

object usePerson {
  def main(args: Array[String]): Unit = {
    val p = new Person("krest", 23)
    println(p.toString)
  }
}