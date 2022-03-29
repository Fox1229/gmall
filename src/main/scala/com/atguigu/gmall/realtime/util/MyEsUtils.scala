package com.atguigu.gmall.realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.apache.zookeeper.proto.CreateRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import scala.collection.mutable.ListBuffer

/**
 * Es工具类
 */
object MyEsUtils {

    /*def main(args: Array[String]): Unit = {

        println(getFields("gmall_dau_info_2022-03-29", "mid"))
    }*/

    val esCli: RestHighLevelClient = builder()

    /**
     * 读取mid
     */
    def getFields(indexName: String, fieldName: String): scala.List[_root_.scala.Predef.String] = {

        // 判断索引是否存在
        val getIndexRequest: GetIndexRequest = new GetIndexRequest(indexName)
        val bool: Boolean = esCli.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
        if(!bool) {
            return null
        }

        // 从ES中提取字段
        val searchRequest: SearchRequest = new SearchRequest(indexName)
        val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
        searchSourceBuilder.fetchSource(fieldName, null).size(10000)
        searchRequest.source(searchSourceBuilder)
        val searchResponse: SearchResponse = esCli.search(searchRequest, RequestOptions.DEFAULT)
        val hits: Array[SearchHit] = searchResponse.getHits.getHits
        val midList: ListBuffer[String] = ListBuffer[String]()
        for (hit <- hits) {
            val sourceAsMap: util.Map[String, AnyRef] = hit.getSourceAsMap
            val mid: String = sourceAsMap.get(fieldName).toString
            midList.append(mid)
        }

        midList.toList
    }

    /**
     * 保存数据到es中
     */
    def saveToEs(indexName: String, data: List[(String, AnyRef)]): Unit = {

        val bulkRequest: BulkRequest = new BulkRequest()
        for ((docId, docObj) <- data) {
            val indexRequest: IndexRequest = new IndexRequest(indexName)
            val jsonStr: String = JSON.toJSONString(docObj, new SerializeConfig(true))
            indexRequest.source(jsonStr, XContentType.JSON)
            indexRequest.id(docId)
            bulkRequest.add(indexRequest)
        }

        esCli.bulk(bulkRequest, RequestOptions.DEFAULT)
    }

    /**
     * 连接es
     */
    def builder(): RestHighLevelClient = {

        val host: String = MyPropUtils(MyConfigUtils.ES_HOST)
        val port: Int = MyPropUtils(MyConfigUtils.ES_PORT).toInt
        val httpHost: HttpHost = new HttpHost(host, port)
        val restClientBuilder: RestClientBuilder = RestClient.builder(httpHost)
        val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
        client
    }

    /**
     * 关闭es
     */
    def close(): Unit = {
        if(esCli != null) {
            esCli.close()
        }
    }
}
