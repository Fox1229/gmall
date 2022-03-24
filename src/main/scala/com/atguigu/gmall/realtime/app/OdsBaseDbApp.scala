package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyRedisUtils, MyKafkaUtils, MyOffsetUtils}
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

        // TODO 准备实时环境
        val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[3]")
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

        val topicName: String = "ODS_BASE_DB_M"
        val groupId: String = "ODS_BASE_DB_M_ID"

        // TODO 从redis中读取偏移量
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

        // TODO 从Kafka中消费数据
        var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsets != null && offsets.nonEmpty) {
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
        } else {
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
        }

        // TODO 提取偏移量结束点
        var offsetRanges: Array[OffsetRange] = null
        val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // TODO 处理数据
        // 5.1 转换数据结构
        val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
            consumerRecord => {
                val dataJson: String = consumerRecord.value()
                val jSONObject: JSONObject = JSON.parseObject(dataJson)
                jSONObject
            }
        )

        //5.2 分流
        //事实表清单
        //val factTables : Array[String] = Array[String]( "order_info","order_detail" /*缺啥补啥*/)
        //维度表清单
        //val dimTables : Array[String] = Array[String]("user_info", "base_province" /*缺啥补啥*/)
        //Redis连接写到哪里???
        // foreachRDD外面:  driver  ，连接对象不能序列化，不能传输
        // foreachRDD里面, foreachPartition外面 : driver  ，连接对象不能序列化，不能传输
        // foreachPartition里面 , 循环外面：executor ， 每分区数据开启一个连接，用完关闭.
        // foreachPartition里面,循环里面:  executor ， 每条数据开启一个连接，用完关闭， 太频繁。
        jsonObjDStream.foreachRDD(
            rdd => {
                //如何动态配置表清单???
                // 将表清单维护到redis中，实时任务中动态的到redis中获取表清单.
                // 类型: set
                // key:  FACT:TABLES   DIM:TABLES
                // value : 表名的集合
                // 写入API: sadd
                // 读取API: smembers
                // 过期: 不过期

                val redisFactKeys: String = "FACT:TABLES"
                val redisDimKeys: String = "DIM:TABLES"
                val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
                //事实表清单
                val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
                //维度表清单
                val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)

                //做成广播变量
                val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
                val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
                jedis.close()

                rdd.foreachPartition(
                    jsonObjIter => {
                        // 开启redis连接
                        val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
                        for (jsonObj <- jsonObjIter) {
                            // 提取操作类型
                            val opType: String = jsonObj.getString("type")

                            val opValue: String = opType match {
                                case "bootstrap-insert" => "I"
                                case "insert" => "I"
                                case "update" => "U"
                                case "delete" => "D"
                                case _ => null
                            }

                            // 判断操作类型: 1. 明确什么操作  2. 过滤不感兴趣的数据
                            if (opValue != null) {
                                // 提取表名
                                val tableName: String = jsonObj.getString("table")
                                val dataObj: JSONObject = jsonObj.getJSONObject("data")

                                if (factTablesBC.value.contains(tableName)) {
                                    // 事实数据
                                    // 提取数据
                                    val dwdTopicName: String = s"DWD_${tableName.toUpperCase}_$opValue"
                                    MyKafkaUtils.send( dwdTopicName, dataObj.toJSONString)

                                    //                                    //模拟数据延迟
                                    //                                    if(tableName.equals("order_detail")){
                                    //                                        Thread.sleep(200)
                                    //                                    }
                                }

                                if (dimTablesBC.value.contains(tableName)) {
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
                                    val id: String = dataObj.getString("id")
                                    val redisKey: String = s"DIM:${tableName.toUpperCase}:$id"
                                    jedis.set(redisKey, dataObj.toJSONString)
                                }
                            }
                        }

                        //关闭redis连接
                        jedis.close()
                        //刷新Kafka缓冲区
                        MyKafkaUtils.flush()
                    }
                )
                //提交offset
                MyOffsetUtils.saveOffset(topicName, groupId, offsetRanges)
            }
        )
        ssc.start()
        ssc.awaitTermination()
    }
}