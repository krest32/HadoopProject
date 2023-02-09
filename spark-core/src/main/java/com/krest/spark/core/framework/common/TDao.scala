package com.krest.spark.core.framework.common

import com.krest.spark.core.framework.util.EnvUtil


trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
