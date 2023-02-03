package com.krest.base.基础

class Person3(xName: String, xAge: Int) {
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

object usePerson3 {
  //  静态会被优先加载
  println("+++++++++++++++++++++++++++++")

  def main(args: Array[String]): Unit = {
    val p = new Person3("krest", 23, "G")
    p.age = 22;
    println(p.age)
    p.sayName()
    println(p.gender)
  }
}