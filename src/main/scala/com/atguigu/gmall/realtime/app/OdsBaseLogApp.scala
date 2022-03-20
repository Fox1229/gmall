package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.bean.{PageActionsLog, PageDisplayLog, PageLog, PageStartLog}
import com.atguigu.gmall.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.lang

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

        val DWD_PAGE_LOG_TOPIC : String = "DWD_PAGE_LOG_TOPIC"  // 页面访问
        val DWD_PAGE_DISPLAY_TOPIC : String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
        val DWD_PAGE_ACTION_TOPIC : String = "DWD_PAGE_ACTION_TOPIC" //页面事件
        val DWD_START_LOG_TOPIC : String = "DWD_START_LOG_TOPIC" // 启动数据
        val DWD_ERROR_LOG_TOPIC : String = "DWD_ERROR_LOG_TOPIC" // 错误数据

        jsonObj.foreachRDD(
            rdd => {
                rdd.foreach(
                    jsonObj => {

                        // 分流过程
                        // TODO 分流错误数据
                        val errObj: JSONObject = jsonObj.getJSONObject("err")
                        if(errObj != null) {
                            // 将错误数据发送到DWD_ERROR_LOG_TOPIC
                            MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toString)
                        } else {
                            // 此时的数据一定存在common字段
                            // 获取公共common数据
                            val commonObj: JSONObject = jsonObj.getJSONObject("common")
                            val ar: String = commonObj.getString("ar")
                            val uid: String = commonObj.getString("uid")
                            val os: String = commonObj.getString("os")
                            val ch: String = commonObj.getString("ch")
                            val is_new: String = commonObj.getString("is_new")
                            val md: String = commonObj.getString("md")
                            val mid: String = commonObj.getString("mid")
                            val vc: String = commonObj.getString("vc")
                            val ba: String = commonObj.getString("ba")
                            val ts: Long = jsonObj.getLong("ts")

                            // TODO 获取页面数据
                            val pageObj: JSONObject = jsonObj.getJSONObject("page")
                            if(pageObj != null) {
                                // 提取page字段
                                val pageId: String = pageObj.getString("page_id")
                                val item: String = pageObj.getString("item")
                                val duringTime: Long = pageObj.getLong("during_time")
                                val itemType: String = pageObj.getString("item_type")
                                val lastPageId: String = pageObj.getString("last_page_id")
                                val sourceType: String = pageObj.getString("source_type")

                                // 封装对象
                                val pageLog: PageLog = PageLog(ar, uid, os, ch, is_new, md, mid, vc, ba, pageId, item,
                                    duringTime, itemType, lastPageId, sourceType, ts)

                                // 发送到DWD_PAGE_LOG_TOPIC
                                // JSON为java代码，默认需要提供get、set方法用来赋值，可以通过传入第二个参数，直接获取字段的值进行赋值
                                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                                // TODO 获取曝光数据
                                val displaysObjArr: JSONArray = jsonObj.getJSONArray("displays")
                                if(displaysObjArr != null && displaysObjArr.size() > 0) {
                                    for(i <- 0 until displaysObjArr.size()) {
                                        val displayObj: JSONObject = displaysObjArr.getJSONObject(i)
                                        val displayType: String = displayObj.getString("display_type")
                                        val displayItem: String = displayObj.getString("item")
                                        val displayItemType: String = displayObj.getString("item_type")
                                        val displayPosId: String = displayObj.getString("pos_id")
                                        val displayOrder: String = displayObj.getString("order")

                                        // 封装对象
                                        val pageDisplayLog: PageDisplayLog = PageDisplayLog(ar, uid, os, ch, is_new, md, mid, vc, ba, pageId, item,
                                            duringTime, itemType, lastPageId, sourceType, displayType,
                                            displayItem, displayItemType, displayPosId, displayOrder, ts)

                                        // 发送数据到DWD_PAGE_DISPLAY_TOPIC
                                        MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                                    }
                                }

                                // TODO 获取事件数据
                                val actionJsonObjArr: JSONArray = jsonObj.getJSONArray("actions")
                                if(actionJsonObjArr != null && actionJsonObjArr.size() > 0) {
                                    for(i <- 0 until actionJsonObjArr.size()) {
                                        val actionJsonObj: JSONObject = actionJsonObjArr.getJSONObject(i)
                                        val actionItem: String = actionJsonObj.getString("item")
                                        val actionId: String = actionJsonObj.getString("action_id")
                                        val actionItemType: String = actionJsonObj.getString("item_type")
                                        val actionItemTs: Long = actionJsonObj.getLong("ts")

                                        // 封装对象
                                        val pageActionsLog: PageActionsLog = PageActionsLog(ar, uid, os, ch, is_new, md, mid, vc, ba, pageId, item,
                                            duringTime, itemType, lastPageId, sourceType, actionItem, actionId,
                                            actionItemType, actionItemTs, ts)

                                        // 发送数据到
                                        MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionsLog, new SerializeConfig(true)))
                                    }
                                }
                            }

                            // TODO 启动数据
                            val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
                            if(startJsonObj != null) {
                                val startEntry: String = startJsonObj.getString("entry")
                                val openAdSkipMs: lang.Long = startJsonObj.getLong("open_ad_skip_ms")
                                val openAdMs: lang.Long = startJsonObj.getLong("open_ad_ms")
                                val loadTime: lang.Long = startJsonObj.getLong("loading_time")
                                val openAdId: lang.Long = startJsonObj.getLong("open_ad_id")

                                // 封装对象
                                val pageStartLog: PageStartLog = PageStartLog(ar, uid, os, ch, is_new, md, mid, vc, ba, startEntry,
                                    openAdSkipMs, openAdMs, loadTime, openAdId, ts)

                                // 发送数据到DWD_START_LOG_TOPIC
                                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(pageStartLog, new SerializeConfig(true)))
                            }
                        }
                    }
                )
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
