package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}
import com.atguigu.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.lang
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 */
object DwdDauApp {

    def main(args: Array[String]): Unit = {

        // 同步ES中的mid到redis
        revertState()

        // TODO 环境准备
        val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("DwdDauApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic: String = "DWD_PAGE_LOG_TOPIC"
        val groupId: String = "DWD_PAGE_LOG_TOPIC_GROUP_ID"

        // TODO 从redis读取偏移量
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topic, groupId)

        // TODO 从kafka消费数据
        var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsets != null && offsets.nonEmpty) {
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId, offsets)
        } else {
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)
        }

        // TODO 提取偏移量结束点
        var offsetRanges: Array[OffsetRange] = null
        val offsetsRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // TODO 转换数据结构
        val pageLogDStream: DStream[PageLog] = offsetsRangesDStream.map(
            consumerRecord => {
                val jsonLog: String = consumerRecord.value()
                val pageLog: PageLog = JSON.parseObject(jsonLog, classOf[PageLog])
                pageLog
            }
        )

        // 去重
        // 自我审查，将页面数据中last_page_id不为空的数据过滤，减少与redis交互次数，减轻redis压力
        val filterDStream: DStream[PageLog] = pageLogDStream.filter(
            pageLog => pageLog.last_page_id == null
        )

        // 第三方审查 所有会话的第一个页面，去redis中检查是否是今天的第一次
        val filterRedisDStream: DStream[PageLog] = filterDStream.mapPartitions(
            pageLogIter => {
                val pageLogList: List[PageLog] = pageLogIter.toList
                println("第三方审查前: " + pageLogList.size)

                //存储要的数据
                val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
                val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
                val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
                for (pageLog <- pageLogList) {
                    // 提取每条数据中的mid (我们日活的统计基于mid， 也可以基于uid)
                    val mid: String = pageLog.mid

                    // 获取日期, 因为我们要测试不同天的数据，所以不能直接获取系统时间
                    val ts: Long = pageLog.ts
                    val date: Date = new Date(ts)
                    val dateStr: String = sdf.format(date)
                    val redisDauKey: String = s"DAU:$dateStr"

                    // redis的判断是否包含操作
                    /*
                        下面代码在分布式环境中，存在并发问题， 可能多个并行度同时进入到if中,导致最终保留多条同一个mid的数据.
                        // list
                        val mids: util.List[String] = jedis.lrange(redisDauKey, 0 ,-1)
                        if(!mids.contains(mid)){
                          jedis.lpush(redisDauKey , mid )
                          pageLogs.append(pageLog)
                        }
                        // set
                        val setMids: util.Set[String] = jedis.smembers(redisDauKey)
                        if(!setMids.contains(mid)){
                          jedis.sadd(redisDauKey,mid)
                          pageLogs.append(pageLog)
                        }
                     */
                    val isNew: lang.Long = jedis.sadd(redisDauKey, mid) // 判断包含和写入实现了原子操作
                    jedis.expire(redisDauKey, 3600 * 24)
                    if (isNew == 1L) {
                        pageLogs.append(pageLog)
                    }
                }
                jedis.close()
                println("第三方审查后: " + pageLogs.size)
                pageLogs.iterator
            }
        )

        // 纬度关联
        val dauInfoDStream: DStream[DauInfo] = filterRedisDStream.mapPartitions(
            pageLogIter => {

                val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
                val dauList: ListBuffer[DauInfo] = new ListBuffer[DauInfo]()
                val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                for (pageLog <- pageLogIter) {

                    val dauInfo: DauInfo = new DauInfo()
                    // 将pageLog中的字段赋值到DauInfo中
                    MyBeanUtils.copyProperties(pageLog, dauInfo)

                    // 补充纬度
                    // 用户性别 年龄
                    val userId: String = pageLog.user_id
                    val userInfo: String = jedis.get(s"DIM:USER_INFO:$userId")
                    val userInfoJsonObj: JSONObject = JSON.parseObject(userInfo)
                    // 获取性别
                    val gender: String = userInfoJsonObj.getString("gender")
                    // 获取生日，计算年龄
                    val birthday: String = userInfoJsonObj.getString("birthday") // 1985-08-16
                    val birthdayLd: LocalDate = LocalDate.parse(birthday)
                    val nowLd: LocalDate = LocalDate.now()
                    val age: Int = Period.between(birthdayLd, nowLd).getYears
                    // 赋值
                    dauInfo.user_gender = gender
                    dauInfo.user_age = age.toString

                    // 地区信息
                    val provinceId: String = dauInfo.province_id
                    val provinceInfo: String = jedis.get(s"DIM:BASE_PROVINCE:$provinceId")
                    val provinceJsonObj: JSONObject = JSON.parseObject(provinceInfo)
                    val provinceName: String = provinceJsonObj.getString("name")
                    val isoCode: String = provinceJsonObj.getString("iso_code")
                    val iso3166: String = provinceJsonObj.getString("iso_3166_2")
                    val areaCode: String = provinceJsonObj.getString("area_code")
                    // 赋值
                    dauInfo.province_name = provinceName
                    dauInfo.province_iso_code = isoCode
                    dauInfo.province_3166_2 = iso3166
                    dauInfo.province_area_code = areaCode

                    // 日期
                    val dateTime: String = sdf.format(new Date(pageLog.ts))
                    val dtAndHr: Array[String] = dateTime.split(" ")
                    val dt: String = dtAndHr(0)
                    val hr: String = dtAndHr(1).split(":")(1)
                    // 赋值
                    dauInfo.dt = dt
                    dauInfo.hr = hr

                    dauList.append(dauInfo)
                }

                jedis.close()
                dauList.iterator
            }
        )

        // TODO 写入ES
        //按照天分割索引，通过索引模板控制mapping、settings、aliases等
        dauInfoDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        val docs: List[(String, DauInfo)] = iter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
                        if (docs.nonEmpty) {
                            // 索引名
                            // 如果是真实的实时环境，直接获取当前日期即可
                            // 因为我们是模拟数据，会生成不同天的数据
                            // 从第一条数据中获取日期
                            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
                            val ts: Long = docs.head._2.ts
                            val dataTime: String = sdf.format(new Date(ts))
                            val indexName: String = s"gmall_dau_info_$dataTime"
                            // 写入es
                            MyEsUtils.saveToEs(indexName, docs)
                        }
                    }
                )

                // TODO 提交offsets
                MyOffsetUtils.saveOffset(topic, groupId, offsetRanges)
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }

    /**
     * ES状态还原
     * 每次启动实时任务时，进行一次状态还原。 以ES为准, 将所以的mid提取出来，覆盖到Redis中
     */
    def revertState(): Unit = {

        // 从ES中查询所有的mid
        val date: LocalDate = LocalDate.now()
        val indexName: String = s"gmall_dau_info_$date"
        val fieldName: String = "mid"
        val mids: List[String] = MyEsUtils.getFields(indexName: String, fieldName: String)

        // 删除redis中保存的所有mid
        val jedis: Jedis = MyRedisUtils.getJedisFromPoll()
        val redisKey: String = s"DAU:$date"
        jedis.del(redisKey)

        // 将从ES中查询到的mid覆盖到Redis中
        if(mids != null && mids.nonEmpty) {

            val pipeline: Pipeline = jedis.pipelined()
            for (mid <- mids) {
                // 添加到批次中
                pipeline.sadd(redisKey, mid)
            }
            pipeline.sync()
        }

        jedis.close()
    }
}
