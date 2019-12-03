package com.atguigu.gmall0624.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0624.bean.{DetailLog, OrderLog, SaleDetail, UserInfo}
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.{MyEsUtil, MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    val orderInfoInputStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
    val orderDetailDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    val userInfoDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER_INFO, ssc)
    val orderDStream: DStream[OrderLog] = orderInfoInputStream.map {
      record => {
        val jsonString: String = record.value()
        //json字符串转成json对象
        val orderlog: OrderLog = JSON.parseObject(jsonString, classOf[OrderLog])
        ////对电话脱敏  138|****|3838
        val tupleTel: (String, String) = orderlog.consignee_tel.splitAt(3)
        val tupleTel2: (String, String) = tupleTel._2.splitAt(4)
        orderlog.consignee_tel = tupleTel._1 + "****" + tupleTel2._2
        //日期
        val date: Array[String] = orderlog.create_time.split(" ")
        orderlog.create_date = date(0)
        orderlog.create_hour = date(1).split(":")(0)
        orderlog
      }
    }
    val orderDetailDstream: DStream[DetailLog] = orderDetailDStream.map {
      record => {
        val jsonString: String = record.value()
        val detailLog: DetailLog = JSON.parseObject(jsonString, classOf[DetailLog])
        detailLog
      }
    }
    val orderInfoWithIdDstream: DStream[(String, OrderLog)] = orderDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithIdDstream: DStream[(String, DetailLog)] = orderDetailDstream.map(detail => (detail.order_id, detail))
    val orderFullJoinedDstream: DStream[(String, (Option[OrderLog], Option[DetailLog]))] = orderInfoWithIdDstream.fullOuterJoin(orderDetailWithIdDstream)

    //双流join
    val saleDetailDStream: DStream[SaleDetail] = orderFullJoinedDstream.flatMap {
      case (id, (orderLogOption, detailLogOption)) => {
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
        val saleDetailListBuffer = ListBuffer[SaleDetail]()
        val jedis: Jedis = RedisUtil.getJedisClient
        if (orderLogOption != None) {
          //1.组合saleDetail，进行join
          val orderlog: OrderLog = orderLogOption.get
          if (detailLogOption != None) {
            val detaillog: DetailLog = detailLogOption.get
            val Saledetail = new SaleDetail(orderlog, detaillog)
            saleDetailListBuffer += Saledetail
          }
          //写缓存
          //redis的key type string key order_info:[id] value order_info_json
          val orderInfoKey: String = "order_info" + orderlog.id
          //转换成json字符串
          val orderLogJson: String = Serialization.write(orderlog)
          jedis.setex(orderInfoKey, 600, orderLogJson)
          //暂存时间
          //读缓存
          val detailLogKey: String = "order_detail" + orderlog.id
          val orderDetailSet: util.Set[String] = jedis.smembers(detailLogKey)
          if (orderDetailSet != null && orderDetailSet.size() > 0) {
            import scala.collection.JavaConversions._
            for (orderDetailJsonString <- orderDetailSet) {
              val detailLog: DetailLog = JSON.parseObject(orderDetailJsonString, classOf[DetailLog])
              val saleDetail: SaleDetail = new SaleDetail(orderlog, detailLog)
              saleDetailListBuffer += saleDetail
            }
          }
        } else {
          if (detailLogOption != None) {
            //写缓存
            //redis的key set key order_detail:[order_id] vale order_detail_json
            val detaillog = detailLogOption.get
            val detailLogKey: String = "order_detail" + detaillog.order_id
            //转换成json字符串
            val orderdetailjson: String = Serialization.write(detaillog)
            jedis.sadd(detailLogKey, orderdetailjson)
            jedis.expire(detailLogKey, 600)
            //暂存时间
            //读缓存
            val orderInfoKey: String = "order_info" + detaillog.order_id
            val orderInfoJsonString: String = jedis.get(orderInfoKey)
            if (orderInfoJsonString != null && orderInfoJsonString.size > 0) {
              val orderInfoJson: OrderLog = JSON.parseObject(orderInfoJsonString, classOf[OrderLog])
              val saleDetail = new SaleDetail(orderInfoJson, detaillog)
              saleDetailListBuffer += saleDetail
            }
          }
        }
        jedis.close()
        saleDetailListBuffer
      }
    }
    //关联维度表user_info
    //使用mappartitions是为了防止多次建立和关闭jedis
    val saleDetailFinalDstream: DStream[SaleDetail] = saleDetailDStream.mapPartitions {
      saleDetailItr => {
        val jedis: Jedis = RedisUtil.getJedisClient
        val detailsBuffer: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
        for (saleDetail <- saleDetailItr) {
          val userKey: String = "user_info" + saleDetail.user_id
          val userJson: String = jedis.get(userKey)
          if (userJson != null && userJson.size > 0) {
            val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
            saleDetail.mergeUserInfo(userInfo)
            detailsBuffer += saleDetail
          }
        }
        jedis.close()
        detailsBuffer.toIterator
      }
    }
    //保存到es中
    saleDetailFinalDstream.foreachRDD{
      rdd=>rdd.foreachPartition{
        orderDetailItr=>{
          val list: List[(String, SaleDetail)] = orderDetailItr.map {
            sale => (sale.order_detail_id, sale)
          }.toList
          MyEsUtil.insertBulk(GmallConstant.ES_INDEX_SALE_DETAIL,list)
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
