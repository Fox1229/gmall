package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object OdsBaseLogApp {

    def main(args: Array[String]): Unit = {

        // 1. 准备环境
        val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("ods_base_log_app")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic: String = "ODS_BASE_LOG"
        val groupId: String = "ODS_BASE_LOG_GROUP_id"
        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)

        val jsonObj: DStream[JSONObject] = kafkaStream.map(
            consumerRecord => {
                val log: String = consumerRecord.value()
                val jsonObj: JSONObject = JSON.parseObject(log)
                jsonObj
            }
        )
        val DWD_ERROR_LOG_TOPIC = "DWD_ERROR_LOG_TOPIC"

        jsonObj.foreachRDD(
            rdd => {
                rdd.foreach(
                    jsonObj => {
                        // 分流过程
                        // 分流错误数据
                        val errObj: JSONObject = jsonObj.getJSONObject("error")
                        if(errObj != null) {
                            // 将错误数据发送到
                            MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toString())
                        } else {

                        }
                    }
                )
            }
        )






        ssc.start()
        ssc.awaitTermination()
    }
}
