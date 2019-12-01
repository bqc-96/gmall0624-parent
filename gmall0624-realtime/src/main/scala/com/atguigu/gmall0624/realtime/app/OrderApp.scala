package com.atguigu.gmall0624.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.bean.OrderLog
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.MykafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
//交易总额
object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(conf,Seconds(5))
    //从kafka获取订单数据
    val inputDstrean: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    //转换数据格式
    val orderDstream: DStream[OrderLog] = inputDstrean.map {
      record => {
        val jsonString: String = record.value()
        //json字符串转换成json对象
        val orderlog: OrderLog = JSON.parseObject(jsonString, classOf[OrderLog])
        //对敏感数据进行脱敏
        //对电话脱敏  138|****|3838
        val tupleTel: (String, String) = orderlog.consignee_tel.splitAt(3)
        val tupleTel2: (String, String) = tupleTel._2.splitAt(4)
        orderlog.consignee_tel = tupleTel._1 + "****" + tupleTel2._2
        //改变日期格式 2020-12-12 12：12：21=>2020-12-12 12
        val date: Array[String] = orderlog.create_time.split(" ")
        orderlog.create_date = date(0)
        orderlog.create_hour = date(1).split(":")(0)
        //订单上加一个字段，该用户是否首次下单
        //维护一个状态，用户是否下过单 /redis 自己做
        orderlog
      }
    }
    //数据发送到hbase
    orderDstream.foreachRDD{
      rdd=>{
        rdd.saveToPhoenix("gmall2019_order_info",
          Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          new Configuration,Some("hadoop102,hadoop103,hadoop104:2181")
        )
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
