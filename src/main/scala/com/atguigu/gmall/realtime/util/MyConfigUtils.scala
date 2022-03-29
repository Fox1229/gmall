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
    val REDIS_HOST: String = "redis_host"
    val REDIS_PORT: String = "redis_port"

    // es
    val ES_HOST: String = "es_host"
    val ES_PORT: String = "es_port"
}
