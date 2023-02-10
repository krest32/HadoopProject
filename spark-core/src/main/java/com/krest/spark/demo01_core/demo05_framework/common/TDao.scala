package com.krest.spark.demo01_core.demo05_framework.common

import com.krest.spark.demo01_core.demo05_framework.util.EnvUtil


trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
