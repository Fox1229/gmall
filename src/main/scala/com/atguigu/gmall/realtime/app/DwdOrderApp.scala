package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

object DwdOrderApp {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("Dwd_Order_App")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val orderInfoTopic: String = "DWD_ORDER_INFO_I"
        val orderInfoGroupId: String = "DWD_ORDER_INFO_I_ID"
        val orderDetailTopic: String = "DWD_ORDER_DETAIL_I"
        val orderDetailGroupId: String = "DWD_ORDER_DETAIL_I_ID"

        // TODO 读取offset
        // orderInfo
        val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderInfoTopic, orderInfoGroupId)
        // orderDetail
        val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderDetailTopic, orderDetailGroupId)

        // TODO 消费数据
        // orderInfo
        var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderInfoOffsets != null && orderInfoOffsets.nonEmpty) {
            orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopic, orderInfoGroupId, orderInfoOffsets)
        } else {
            orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopic, orderInfoGroupId)
        }

        // orderDetail
        var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderInfoOffsets != null && orderInfoOffsets.nonEmpty) {
            orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopic, orderDetailGroupId, orderDetailOffsets)
        } else {
            orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopic, orderDetailGroupId)
        }

        // TODO 获取消费后的offset
        var orderInfoOffsetRanges: Array[OffsetRange] = null
        val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
            rdd => {
                orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // TODO 获取消费后的offset
        var orderDetailOffsetRanges: Array[OffsetRange] = null
        val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
            rdd => {
                orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // TODO 转换结构
        // orderInfo
        val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
            consumerRecord => {
                val orderInfoStr: String = consumerRecord.value()
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                orderInfo
            }
        )

        // orderDetail
        val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
            consumerRecord => {
                val orderDetailStr: String = consumerRecord.value()
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
                orderDetail
            }
        )

//        orderInfoDStream.print()
//        orderDetailDStream.print()

        // TODO 纬度关联
        val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
            orderInfoIter => {
                val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
                val orderInfoList: List[OrderInfo] = orderInfoIter.toList
                for (orderInfo <- orderInfoList) {

                    // 时间处理
                    val createTime: String = orderInfo.create_time // 2020-06-10 17:46:33
                    val dtAndHr: Array[String] = createTime.split(" ")
                    val dt: String = dtAndHr(0)
                    val hr: String = dtAndHr(1).split(":")(0)
                    orderInfo.create_date = dt
                    orderInfo.create_hour = hr

                    // 地区处理
                    val provinceId: Long = orderInfo.province_id
                    val provinceKey: String = s"DIM:BASE_PROVINCE:$provinceId"
                    val provinceJson: String = jedis.get(provinceKey)
                    val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
                    val provinceName: String = provinceJsonObj.getString("name")
                    val areaCode: String = provinceJsonObj.getString("area_code")
                    val iso3166: String = provinceJsonObj.getString("iso_3166_2")
                    val isoCode: String = provinceJsonObj.getString("iso_code")
                    orderInfo.province_name = provinceName
                    orderInfo.province_area_code = areaCode
                    orderInfo.province_3166_2_code = iso3166
                    orderInfo.province_iso_code = isoCode

                    // 用户处理
                    val userId: Long = orderInfo.user_id
                    val userKey: String = s"DIM:USER_INFO:$userId"
                    val userJson: String = jedis.get(userKey)
                    val userJsonObj: JSONObject = JSON.parseObject(userJson)
                    val birthday: String = userJsonObj.getString("birthday")
                    val birthdayLd: LocalDate = LocalDate.parse(birthday)
                    val nowLd: LocalDate = LocalDate.now()
                    val age: Int = Period.between(birthdayLd, nowLd).getYears
                    val gender: String = userJsonObj.getString("gender")
                    orderInfo.user_age = age
                    orderInfo.user_gender = gender
                }
                jedis.close()
                orderInfoList.iterator
            }
        )

        // orderInfoDimDStream.print()
        // TODO 双流Join
        // 内连接 join  结果集取交集
        // 外连接
        //   左外连  leftOuterJoin   左表所有+右表的匹配  , 分析清楚主(驱动)表 从(匹配表) 表
        //   右外连  rightOuterJoin  左表的匹配 + 右表的所有,分析清楚主(驱动)表 从(匹配表) 表
        //   全外连  fullOuterJoin   两张表的所有

        // 从数据库层面： order_info 表中的数据 和 order_detail表中的数据一定能关联成功.
        // 从流处理层面:  order_info 和  order_detail是两个流， 流的join只能是同一个批次的数据才能进行join
        //               如果两个表的数据进入到不同批次中， 就会join不成功.
        // 数据延迟导致的数据没有进入到同一个批次，在实时处理中是正常现象. 我们可以接收因为延迟导致最终的结果延迟.
        // 我们不能接收因为延迟导致的数据丢失.
        val orderInfoKVDStream: DStream[(Long, OrderInfo)] = orderInfoDimDStream.map(
            orderInfo => (orderInfo.id, orderInfo)
        )

        val orderDetailKVDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(
            orderDetail => (orderDetail.order_id, orderDetail)
        )

        // 解决:
        //  1. 扩大采集周期， 治标不治本
        //  2. 使用窗口, 治标不治本, 还要考虑数据去重、 Spark状态的缺点
        //  3. 首先使用fullOuterJoin,保证join成功或者没有成功的数据都出现到结果中.
        //     让双方都多两步操作, 到缓存中找对的人， 把自己写到缓存中
        val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))]
            = orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)

        val orderWideDSteam: DStream[OrderWide] = orderJoinDStream.mapPartitions(
            iter => {

                val listBuffer: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
                val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
                for ((key, (orderInfoOp, orderDetailOp)) <- iter) {

                    // orderInfo 有， orderDetail 有
                    // 添加到集合写出
                    if (orderInfoOp.isDefined) {

                        val orderInfoJson: OrderInfo = orderInfoOp.get
                        if (orderDetailOp.isDefined) {
                            val orderDetailJson: OrderDetail = orderDetailOp.get
                            val orderWide: OrderWide = new OrderWide(orderInfoJson, orderDetailJson)
                            listBuffer.append(orderWide)
                        }

                        // orderInfo 有， orderDetail 没有

                        // orderInfo写缓存
                        // orderInfo 在本次不论是否关联上，都需要写入缓存
                        // 类型: string
                        // key: ORDERJOIN:ORDER_INFO:ORDER_ID
                        // value: json
                        // 写入: set
                        // 读取: get
                        // 过期: 24h
                        val redisKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfoJson.id}"
                        jedis.set(redisKey, JSON.toJSONString(orderInfoJson, new SerializeConfig(true)))
                        jedis.expire(redisKey, 24 * 3600)

                        // 读取redis缓存，是否有需要join的orderDetail
                        val redisOdKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfoJson.id}"
                        val orderDetails: util.Set[String] = jedis.smembers(redisOdKey)
                        if (orderDetails != null && orderDetails.size() > 0) {
                            import scala.collection.JavaConverters._
                            for (orderDetail <- orderDetails.asScala) {
                                val od: OrderDetail = JSON.parseObject(orderDetail, classOf[OrderDetail])
                                val orderWide: OrderWide = new OrderWide(orderInfoJson, od)
                                listBuffer.append(orderWide)
                            }
                        }
                    } else {
                        // orderInfo 没有， orderDetail 有
                        // 读缓存，判断是否有需要join的orderInfo， 若存在，join
                        val orderDetail: OrderDetail = orderDetailOp.get
                        val orderInfo: String = jedis.get(s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}")
                        if (orderInfo != null && orderInfo.nonEmpty) {
                            val orderInfoObj: OrderInfo = JSON.parseObject(orderInfo, classOf[OrderInfo])
                            val orderWide: OrderWide = new OrderWide(orderInfoObj, orderDetail)
                            listBuffer.append(orderWide)
                        } else {
                            // orderDetail写缓存
                            // 类型: set
                            // key: ORDERJOIN:ORDER_DETAIL:ORDER_ID
                            // value: json, json...
                            // 写入: sadd
                            // 读取: smembers
                            // 过期: 24h
                            val redisKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
                            jedis.sadd(redisKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
                            jedis.expire(redisKey, 24 * 3600)
                        }
                    }
                }
                jedis.close()
                listBuffer.iterator
            }
        )

        // TODO 写入ES
        orderWideDSteam.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        val orderWideList: List[(String, OrderWide)]
                            = iter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
                        if(orderWideList.nonEmpty) {
                            val dt: String = orderWideList.head._2.create_date
                            val indexName: String = s"gmall_order_wide_$dt"
                            MyEsUtils.saveToEs(indexName, orderWideList)
                        }
                    }
                )

                // 提交offset
                MyOffsetUtils.saveOffset(orderInfoTopic, orderInfoGroupId, orderInfoOffsetRanges)
                MyOffsetUtils.saveOffset(orderDetailTopic, orderDetailGroupId, orderDetailOffsetRanges)
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
