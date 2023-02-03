package com.krest.base.方法与函数

trait Read {
  def read(name: String): Unit = {
    println(name + " is reading")
  }
}

trait Listen {
  def listen(name: String): Unit = {
    println(name + " is listening")
  }
}

// 多个继承
class Person() extends Read with Listen {

}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val person = new Person
    person.read("krest")
    person.listen("kara")
  }
}
