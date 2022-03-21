package com.atguigu.gmall.realtime.util

import java.util.ResourceBundle

object MyPropUtils {

    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

    def apply(key: String): String = {
        bundle.getString(key)
    }
}
