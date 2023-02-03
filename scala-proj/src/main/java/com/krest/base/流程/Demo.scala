package com.krest.base.流程

object Demo {
  def main(args: Array[String]): Unit = {

    // if 语句
    val age = 21
    if (age == 20) {
      println("age = 20 ")
    } else {
      println("age != 20")
    }

    // 定义一个 range 集合，包含 10
    val i = 1 to 10
    // 定义一个 range 集合，不包含 10
    val j = 1 until 10
    println(i)
    println(j)
    //    for 循环
    for (i <- 1 until 10) {
      println(i)
    }

    // 从 1 到 10 ，步长是2
    for (i <- 1.to(10, 2)) {
      println(i)
    }

    // 99乘法表 基本寫法
    for (i <- 1 until 10) {
      for (j <- 1 until 10) {
        if (i >= j) {
          print(i + " * " + j + " = " + i * j + " \t")
        }
        if (i == j) {
          println();
        }
      }
    }

    // 99乘法表 优化寫法
    for (i <- 1 until 10; j <- 1 until 10) {
      if (i >= j) {
        print(i + " * " + j + " = " + i * j + " \t")
      }
      if (i == j) {
        println();
      }
    }

    // 复杂循环
    for (i <- 1 to 100; if (i % 2 == 0); if (i >= 90)) {
      println(i)
    }

    // 生成了一个集合
    var res = for (i <- 1 to 100; if (i % 2 == 0); if (i >= 90)) yield i
    println(res)

    var num = 1
    while (num < 5) {
      println(num)
      num = num + 1
    }
  }
}
