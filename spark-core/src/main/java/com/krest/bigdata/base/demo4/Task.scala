package com.krest.bigdata.base.demo4

class Task extends Serializable {
  val datas = List(1, 2, 3, 4)

  val logic = (num: Int) => {
    num * 2
  }

}
