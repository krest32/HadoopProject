package com.krest.base.基础

class Person4(xName: String, xAge: Int) {
  // 定义属性，默认访问权限是 public
  val name = xName
  var age = xAge
  var gender = "M"

  println("############################3")

  // 重写构造方法，在 new 一个对象的时候，这个类除了构造方法不执行，其他方法都会被执行
  def this(yName: String, yAge: Int, yGender: String) {
    this(yName, yAge)
    this.gender = yGender
  }

  def sayName(): Unit = {
    println(name)
  }
}

object usePerson4 {

  // 每个类都有这么一个默认的方法,apply方法可以又多个
  def apply(i: Int) {
    println("score is " + i)
  }

  // 每个类都有这么一个默认的方法
  def apply(i: String) {
    println("name is " + i)
  }

  def main(args: Array[String]): Unit = {
    // 伴生类
    usePerson4(1000)
    usePerson4("krest")
  }
}