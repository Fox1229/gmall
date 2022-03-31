package com.atguigu.gmall.realtime.util

import java.util
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * Kafka工具类，用于生产和消费
 */
object MyKafkaUtils {

    private val consumerConfig: mutable.Map[String, Object] = mutable.Map[String, Object](
        // kafka集群位置
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropUtils(MyConfigUtils.KAFKA_BOOTSTRAP_SERVER),
        // kv反序列化
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> MyPropUtils(MyConfigUtils.KEY_DESERIALIZER_CLASS),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> MyPropUtils(MyConfigUtils.VALUE_DESERIALIZER_CLASS),
        // group id
        // offset提交
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
        // 自动提交时间间隔：5s
        // ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> ""
        // offset重置
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    )

    /**
     * 基于SparkStreaming消费，获取kafkaDStream
     */
    def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]]
            = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig))
        kafkaDStream
    }

    /**
     * 指定offset消费
     */
    def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]) = {
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]]
        = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig, offsets))
        kafkaDStream
    }

    /**
     * 生产者对象
     */
    val producer: KafkaProducer[String, String] = createKafkaProducer()

    /**
     * 创建生产者对象
     */
    def createKafkaProducer() : KafkaProducer[String, String] = {

        val producerConfig: util.HashMap[String, Object] = new util.HashMap[String, Object]()
        // 集群位置
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropUtils(MyConfigUtils.KAFKA_BOOTSTRAP_SERVER))
        // 序列化
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MyPropUtils(MyConfigUtils.KEY_SERIALIZER_CLASS))
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyPropUtils(MyConfigUtils.VALUE_SERIALIZER_CLASS))
        // acks:响应级别
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
        // batch.size: 16K
        // linger.ms: 0
        // retries
        // 幂等配置
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

        val kafkaDStream: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfig)
        kafkaDStream
    }

    /**
     * 生产数据(使用粘性分区)
     */
    def send(topic: String, msg: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, msg))
    }

    /**
     * 生产数据(使用key分区)
     */
    def send(topic: String, key: String, msg: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, key, msg))
    }

    /**
     * 关闭生产者对象
     */
    def closeProducer: Unit = {
        if(producer != null) {
            producer.close()
        }
    }

    /**
     * 将缓冲区数据刷写到磁盘
     */
    def flush(): Unit = {
        if(producer != null) {
            producer.flush()
        }
    }
}
