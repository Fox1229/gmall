package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis工具类，获取Redis连接
 */
object MyJedisUtils {

    var jedisPool: JedisPool = null

    def getJedisFromPoll(): Jedis = {

        if (jedisPool == null) {
            val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
            jedisPoolConfig.setMaxTotal(100) //最大连接数
            jedisPoolConfig.setMaxIdle(20) //最大空闲
            jedisPoolConfig.setMinIdle(20) //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长毫秒
            jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

            // 获取host和port
            val host: String = MyPropUtils(MyConfigUtils.redis_host)
            val port: String = MyPropUtils(MyConfigUtils.redis_port)

            jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
        }

        jedisPool.getResource
    }
}
