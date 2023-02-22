package com.krest.base.方法与函数

import java.util.Date

import javafx.scene.chart.PieChart.Data

object Demo {
  def main(args: Array[String]): Unit = {
    /**
     * 1. 定义方法
     * 可以自动定一返回值
     * 方法返回值需要添加 =
     */
    //    def max(a: Int, b: Int) = {
    //      if (a > b) a else b
    //    }
    //    println(max(100, 20))

    /**
     * 2. 递归方法
     * 需要手动指定数据的返回类型
     */
    //    def func(num: Int): Int = {
    //      if (num == 1) 1 else num * func(num - 1)
    //    }


    /**
     * 3. 参数有默认值的方法,也同样需要指定返回的类型
     */
    //    def func(a: Int = 10, b: Int = 20): Int = {
    //      a * b
    //    }
    //
    //    println(func())

    /**
     * 4. 可变长参数的方
     */
    //    def func(str: Any*): Unit = {
    //      for (elem <- str) {
    //        println(elem)
    //      }

    //      第二种打印方式
    //      str.foreach(println(_))

    //  }
    //    func("hello", "b", "c")


    /**
     * 5. 匿名函数
     * 方法的参数是一个函数的时候，常用
     */
    //    var num: String => Unit = (s: String) => {
    //      println(s)
    //    }
    //    num("aaa")

    /**
     * 嵌套函数
     */
    //    def fun(num: Int): Int = {
    //      def funSub(a: Int): Int = {
    //        if (a == 1) {
    //          a
    //        } else {
    //          a * fun(a - 1)
    //        }
    //      }
    //
    //      funSub(num)
    //    }
    //    println(fun(5))

    /**
     * 7. 偏应用函数
     * 某些情况下，参数非常多，每次调用只有某个固定的参数，那么就可以使用这个
     */
    //    def showLog(date: Date, log: String): Unit = {
    //      println(s"date is $date  log is :$log")
    //    }
    //
    //    val date = new Date()
    //
    //    def fun = showLog(date, _: String)
    //
    //    fun("aaa")
    //    fun("bbb")
    //    fun("ccc")

    /**
     * 8. 高阶函数
     * 方法的参数和接收可能都是函数
     */
    //      入参是函数
    //    def add(a: Int, b: Int): Int = {
    //      a + b
    //    }
    //
    //    def func(f: (Int, Int) => Int, s: String): String = {
    //      val i = f(100, 200)
    //      i + "#" + s
    //    }
    //
    //    // 第一种调用方式
    //    val str = func(add, "scala")
    //    println(str)
    //    //第二种调用方式
    //    println(func((a: Int, b: Int) => {
    //      a * b
    //    }, "scala"))


    //    出参数函数
    //    def func(s: String): (String, String) => String = {
    //      def func1(s1: String, s2: String): String = {
    //        s1 + "~" + s2 + "#" + s
    //      }
    //
    //      func1
    //    }
    //
    //    println(func("a")("b", "c"))
  }




}
