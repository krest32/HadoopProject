package com.krest.base.基础

class Person2(xName: String, xAge: Int) {
  // 定义属性
  val name = xName
  var age = xAge
  def sayName(): Unit ={
    println(name)
  }
}

object usePerson2 {
  def main(args: Array[String]): Unit = {
    val p = new Person2("krest", 23)
    p.age=22;
    println(p.age)
    p.sayName()
  }
}