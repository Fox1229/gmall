package com.atguigu.gmall.realtime.util

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * 对象拷贝工具类
 */
object MyBeanUtils {

    /**
     * 将源对象的属性值拷贝到目标对象的属性上
     *
     * @param srcObj  源对象
     * @param destObj 目标对象
     */
    def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {

        if (srcObj == null || destObj == null) {
            return
        }

        // 获取srcObj所有属性
        val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields
        for (srcField <- srcFields) {
            Breaks.breakable {
                // scala中的get、set方法名称
                // get: 属性名
                // set: 属性名_$eq
                val getMethodName: String = srcField.getName
                val setMethodName: String = srcField.getName + "_$eq"

                // 从scrObj中获取get方法对象
                val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
                // 从destObj中获取set方法对象
                // 目标对象中可能不存在源对象中的字段，需要进行判断
                val setMethod: Method =
                    try {
                        destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
                    } catch {
                        // NoSuchMethodException
                        case ex: Exception => Breaks.break()
                    }

                // 忽略对final属性的修改
                val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
                if(destField.getModifiers.equals(Modifier.FINAL)) {
                    Breaks.break()
                }

                // 调用get方法获取到srcObj属性的值， 再调用set方法将获取到的属性值赋值给destObj的属性
                setMethod.invoke(destObj, getMethod.invoke(srcObj))
            }
        }


    }
}
