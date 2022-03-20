package com.atguigu.gmall.realtime.util

/**
 * 配置类
 */
object MyConfigUtils {

    val KAFKA_BOOTSTRAP_SERVER: String = "kafka.broker.list"
    val KEY_DESERIALIZER_CLASS: String = "kafka.deserializer.key"
    val VALUE_DESERIALIZER_CLASS: String = "kafka.deserializer.value"
    val KEY_SERIALIZER_CLASS: String = "kafka.serializer.key"
    val VALUE_SERIALIZER_CLASS: String = "kafka.serializer.value"
}
