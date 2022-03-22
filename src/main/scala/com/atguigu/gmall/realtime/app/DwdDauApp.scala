package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.PageLog
import com.atguigu.gmall.realtime.util.{MyJedisUtils, MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.lang
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 */
object DwdDauApp {

    def main(args: Array[String]): Unit = {

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
        val pageLogFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
            pageLogIter => {
                //获取redis连接
                val jedis: Jedis = MyJedisUtils.getJedisFromPoll()
                val filterList: ListBuffer[PageLog] = ListBuffer[PageLog]()
                val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
                println("过滤前 : " + pageLogIter.size)
                for (pageLog <- pageLogIter) {
                    val dateStr: String = sdf.format(new Date(pageLog.ts))
                    val dauKey: String = s"DAU:$dateStr"
                    val ifNew: lang.Long = jedis.sadd(dauKey, pageLog.mid)
                    //设置过期时间
                    jedis.expire(dauKey,3600 * 24)
                    if (ifNew == 1L) {
                        filterList.append(pageLog)
                    }
                }
                jedis.close()
                println("过滤后: " + filterList.size)
                filterList.toIterator
            }
        )

//        pageLogFilterDStream.print(10)

        // TODO 写入ES
        // TODO 提交offsets

        ssc.start()
        ssc.awaitTermination()
    }
}
