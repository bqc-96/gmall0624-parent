package com.atguigu.gmall0624.realtime.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool:JedisPool=null
def getJedisClient :Jedis={
  //开辟一i个连接池
  if(jedisPool==null){
    val config: Properties = PropertiesUtil.load("config.properties")

    val host: String = config.getProperty("redis.host")
    val port: String = config.getProperty("redis.port")
    val jedisPoolconfig= new JedisPoolConfig()
    jedisPoolconfig.setMaxTotal(100)//最大连接数
    jedisPoolconfig.setMaxIdle(20)//最大空闲
    jedisPoolconfig.setMinIdle(20)//最小空闲
    jedisPoolconfig.setBlockWhenExhausted(true)//忙碌时是否等待
    jedisPoolconfig.setMaxWaitMillis(500)//忙碌时等待时长
    jedisPoolconfig.setTestOnBorrow(true)//每次获得连接的进行测试
    jedisPool = new JedisPool(jedisPoolconfig,host,port.toInt)
  }
  jedisPool.getResource
}
}
