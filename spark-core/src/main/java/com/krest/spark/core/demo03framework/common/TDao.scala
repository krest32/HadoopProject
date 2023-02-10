package com.krest.spark.core.demo03framework.common

import com.krest.spark.core.demo03framework.util.EnvUtil


trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
