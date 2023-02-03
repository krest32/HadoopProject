package com.krest.bigdata.base.demo4

class SubTask extends Serializable {


  var datas: List[Int] = _

  var logic: (Int) => Int = _

  // 准备方法
  def compute(): List[Int] = {
    datas.map(logic)
  }

}
