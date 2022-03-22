package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyJedisUtils, MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

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
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topic, groupId)

        // TODO 读取ODS_BASE_DB_M数据
        var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if(offsets != null && offsets.nonEmpty) {
            // 获取到offset，从指定offset消费
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId, offsets)
        } else {
            // 没有获取到offset，从默认offset消费
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)
        }

        // TODO 从消费到的数据中获取offset结束点
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
        // 事实数据 => kafka
        // 纬度数据 => redis
        jsonObjDStream.foreachRDD(
            rdd => {

                // 如何动态配置表清单???
                // 将表清单维护到redis中，实时任务中动态的到redis中获取表清单.
                // 类型: set
                // key:  FACT:TABLES   DIM:TABLES
                // value : 表名的集合
                // 写入API: sadd
                // 读取API: smembers
                // 过期: 不过期
                // TODO 动态配置表清单：将表清单维护到redis中
                val redisFactKeys: String = "FACT:TABLES"
                val redisDimKeys: String = "DIM:TABLES"
                val jedis: Jedis = MyJedisUtils.getJedisFromPoll()
                // 事实表
                val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
                // 维度表
                val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)
                // 添加到广播变量，避免重复发送
                val factTableBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
                val dimTableBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
                jedis.close()

//                println("factTables " + factTables)
//                println("dimTables " + dimTables)

                // Redis连接写到哪里???
                // foreachRDD外面: driver，连接对象不能序列化，不能传输
                // foreachRDD里面, foreachPartition外面: driver，连接对象不能序列化，不能传输
                // foreachPartition里面, 循环外面：executor，每分区数据开启一个连接，用完关闭.
                // foreachPartition里面,循环里面: executor，每条数据开启一个连接，用完关闭，太频繁。
                rdd.foreachPartition(
                    jsonObjIter => {

                        val jedis: Jedis = MyJedisUtils.getJedisFromPoll()

                        for (jsonObj <- jsonObjIter) {

                            // 获取操作类型
                            val opType: String = jsonObj.getString("type")
                            val opValue: String = opType match {
                                case "bootstrap-insert" => "I"
                                case "input" => "I"
                                case "update" => "U"
                                case "delete" => "D"
                                case _ => null
                            }

                            val dataJson: JSONObject = jsonObj.getJSONObject("data")
                            if(opValue != null) {

                                // 获取表名
                                val tableName: String = jsonObj.getString("table")

                                // 事实数据发送到kafka
                                if(factTableBC.value.contains(tableName)) {
                                    // 获取数据
                                    val id: String = dataJson.getString("id")
                                    val tmpTopic: String = s"DWD_${tableName.toUpperCase()}_${opValue}"
                                    MyKafkaUtils.send(tmpTopic, id, dataJson.toJSONString)
                                }

                                // 维度数据
                                // 类型 : string  hash
                                //        hash ： 整个表存成一个hash。要考虑目前数据量大小和将来数据量增长问题及高频访问问题.
                                //        hash :  一条数据存成一个hash. 会将每个字段单独保存，对于不是频繁读取某条数据、某个字段的场景，显得没有必要。
                                //                而且对于每次获取多个字段的场景
                                //              key：表名
                                //              field：主键
                                //              value：每个字段
                                //        String : 一条数据存成一个jsonString. 便于获取多个字段
                                // key :  DIM:表名:ID
                                // value : 整条数据的jsonString
                                // 写入API: set
                                // 读取API: get
                                // 过期:  不过期
                                // 提取数据中的id
                                // 纬度数据发送到redis
                                if(dimTableBC.value.contains(tableName)) {
                                    val id: String = dataJson.getString("id")
                                    val key: String = s"DIM:${tableName.toUpperCase}:${id}"
                                    jedis.set(key, dataJson.toJSONString)
                                }
                            }
                        }

                        // 关闭redis连接
                        jedis.close()
                        // TODO 刷写数据
                        MyKafkaUtils.flush()
                    }
                )

                // TODO 提交偏移量
                MyOffsetUtils.saveOffset(topic, groupId, offsetRanges)
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
