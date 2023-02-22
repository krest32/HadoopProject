package com.krest.base.集合

import scala.collection.mutable.ArrayBuffer

object ArrayListDemo {
  def main(args: Array[String]): Unit = {
    //    val arr = Array[String]("a", "b", "c", "d")
    //    第一种遍历方式
    //    arr.foreach(element => {
    //      println(element)
    //    })
    //    第二种遍历方式
    //    arr.foreach(println)

    //    3代表数组的长度
    //    val arr = new Array[Int](3)
    //    arr(0) = 1
    //    arr(1) = 2
    //    arr(2) = 3
    //    arr.foreach(println)


    //    2维数组
    //    val arr = new Array[Array[Int]](3)
    //    arr(0) = Array[Int](1, 2, 3)
    //    arr(1) = Array[Int](4, 5, 6)
    //    arr(2) = Array[Int](7, 8, 9)
    //  第一种遍历
    //    for (tempArr <- arr) {
    //      for (element <- tempArr) {
    //        println(element)
    //      }
    //    }

    //    第二种遍历
    //    for (tempArr <- arr; element <- tempArr) {
    //      println(element)
    //    }

    //    第三种遍历
    //    arr.foreach(tempArr => {
    //      tempArr.foreach(println)
    //    })


    //    数组拼接
    //    val arr1 = Array[String]("a", "b", "c");
    //    val arr2 = Array[String]("d", "e", "f");
    //    val arrays: Array[String] = Array.concat(arr1, arr2);
    //    arrays.foreach(println)


    //    自动填充
    //    val strings = Array.fill(5)("hello")
    //    strings.foreach(println)


    //    可变集合
    import scala.collection.mutable;
    //    不可变集合
    import scala.collection.immutable;


    //    可变集合
    //    val arr = ArrayBuffer[Int](1, 2, 3, 4)
    //    //    像元素数组后进行追加
    //    arr.+=(5)
    //    //    向头部追加元素
    //    arr.+=:(100)
    //    //    追加多个
    //    arr.append(6, 7, 8)
    //    arr.foreach(println)


    //    不可变集合
    //    val list = List[String]("hello scala", "hello java", "hello spark")

    //    切分成多个数组
    //    val strings: List[Array[String]] = list.map(s => {
    //      s.split(" ")
    //    })


    //    所有字符切分一个数组
    //    val strings: List[String] = list.flatMap(s => {
    //      s.split(" ")
    //    })
    //    strings.foreach(println)

    // 整体当成字符串
    //    val str = list.+("hello golang")
    //    println(str)
  }
}
