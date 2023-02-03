package com.krest.base.方法与函数

/**
 * 样例类，默认参数有 get set 方法
 * 还实现了 toString equal copy 等方法
 *
 * @param name
 * @param age
 */
case class Human(name: String, age: Int) {

}

object Demo4 {
  def main(args: Array[String]): Unit = {
    val p1 = new Human("krest", 10)
    val p2 = new Human("krest", 10)
    println(p1.equals(p2))
  }
}
