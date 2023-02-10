package com.krest.spark.demo01_core.test

class SubTask extends Serializable {
    var datas : List[Int] = _
    var logic : (Int)=>Int = _

    // 计算
    def compute() = {
        datas.map(logic)
    }
}
