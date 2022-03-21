package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 业务数据分流
 */
object OdsBaseDbApp {

    def main(args: Array[String]): Unit = {

        // TODO 环境准备
        val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("ODS_BASE_DB_M")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic: String = "ODS_BASE_DB_M"
        val groupId: String = "ODS_BASE_DB_M_ID"

        // TODO 从redis读取offset
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.getOffset(topic, groupId)

        // TODO 读取ODS_BASE_DB_M数据
        var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if(offsets != null && offsets.nonEmpty) {
            // 获取到offset，从指定offset消费
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId, offsets)
        } else {
            // 没有获取到offset，从默认offset消费
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)
        }

        // TODO 从消费到的数据中获取offset
        var offsetRanges: Array[OffsetRange] = null
        val offsetRangeDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // TODO 转化数据结构
        val jsonObjDStream: DStream[JSONObject] = offsetRangeDStream.map(
            consumerRecord => {
                val log: String = consumerRecord.value()
                val jsonObj: JSONObject = JSON.parseObject(log)
                jsonObj
            }
        )

        // TODO 分流
        // 事实数据
        // 纬度数据
        jsonObjDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    jsonObjIter => {
                        for (jsonObj <- jsonObjIter) {

                        }
                        // 刷写数据
                        MyKafkaUtils.flush()
                    }
                )

                // 提交偏移量
                MyOffsetUtils.saveOffset(topic, groupId, offsetRanges)
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
