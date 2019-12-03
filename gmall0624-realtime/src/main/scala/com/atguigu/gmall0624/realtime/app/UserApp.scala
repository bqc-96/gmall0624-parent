package com.atguigu.gmall0624.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.bean.UserInfo
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.{MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

object UserApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val userInfoInputDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER_INFO, ssc)
    val userInfoDStream: DStream[UserInfo] = userInfoInputDStream.map {
      record => {
        val jsonString: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(jsonString, classOf[UserInfo])
        userInfo
      }
    }
    //写入redis
    userInfoDStream.foreachRDD{
      rdd=>{
          rdd.foreachPartition{
            userInfo=>{
              val jedis: Jedis = RedisUtil.getJedisClient
              implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
              //redis type string key user_info:user_id value user_info_json
              for (user <- userInfo) {
                val userkey: String = "user_info"+user.id
                //转换成json字符串
                val userJson: String = Serialization.write(user)
                jedis.set(userkey,userJson)
              }
              jedis.close()
            }
          }
      }
    }

    ssc.start()
    ssc.awaitTermination()


  }
}
