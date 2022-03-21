package com.atguigu.gmall.realtime.util

/**
 * 配置类
 */
object MyConfigUtils {

    // kafka
    val KAFKA_BOOTSTRAP_SERVER: String = "kafka.broker.list"
    val KEY_SERIALIZER_CLASS: String = "kafka.serializer.key"
    val VALUE_SERIALIZER_CLASS: String = "kafka.serializer.value"
    val KEY_DESERIALIZER_CLASS: String = "kafka.deserializer.key"
    val VALUE_DESERIALIZER_CLASS: String = "kafka.deserializer.value"

    // redis
    val redis_host: String = "redis_host"
    val redis_port: String = "redis_port"
}
