package com.atguigu.gmall0624.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MykafkaUtil {
  private val prop: Properties = PropertiesUtil.load("config.properties")
  val broker_list: String = prop.getProperty("kafka.broker.list")//根据key获取值
  //kafka消费者配置
  val kafkaParam=Map(
    "bootstrap.servers"-> broker_list,//初始化连接到集群地址
    "key.deserializer"->classOf[StringDeserializer],
    "value.deserializer"->classOf[StringDeserializer],
    "group.id"->"gmall_consumer_group",//用于标识属于哪个消费者群体
    "auto.offset.reset"->"latest", //latest会自动重置偏移量为最新偏移量
    "enable.auto.commit"->(true:java.lang.Boolean)
  )
 //创建DStream：返回接收到的数据
  //locationStrategies:根据给定的主题和集群创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题
   def getKafkaStream(topic :String,ssc :StreamingContext):InputDStream[ConsumerRecord[String,String]]={
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam))
  dStream
  }
}
