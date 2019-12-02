package com.atguigu.gmall0624.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.bean.{AlertInfo, AlterLog}
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.{MyEsUtil, MykafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.parse.MinAggregateParseNode
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(conf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)
    //分组同一mid,先转换格式
    //转换格式
    val eventInfoDStream: DStream[AlterLog] = inputDstream.map {
      record => {
        val jsonString: String = record.value()
        val alterLog: AlterLog = JSON.parseObject(jsonString, classOf[AlterLog])
        //转换成json对象
        val date = new Date(alterLog.ts)
        val formattor = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateString: String = formattor.format(date)
        val dateArr: Array[String] = dateString.split(" ")
        alterLog.logDate = dateArr(0)
        alterLog.logHour = dateArr(1)
        alterLog
      }
    }
    //开窗口，5分内，滑动步长5s
    val windowDStream: DStream[AlterLog] = eventInfoDStream.window(Seconds(300),Seconds(5))
    //分组按mid
    val groupByMid: DStream[(String, Iterable[AlterLog])] = windowDStream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()
    val alertInfoDStream: DStream[(Boolean, AlertInfo)] = groupByMid.map {
      case (mid, eventInfoItr) => {
        //a.三次及以上用不同账号登陆并领取优惠卷，得到窗口内mid的操作集合，判断集合是否符合预警规则
        //b.并且在登陆到领卷过程中没有浏览商品
        val eventList = new util.ArrayList[String]()
        //存储所有的行为
        val uidSet = new util.HashSet[String]()
        //存储同一mid的所有uid,利用set去重
        val itemSet = new util.HashSet[String]()
        //存储同一mid下的商品id
        var isAlert = false
        //是否预警的标签
        var isClickItem = false //是否浏览商品的标签
        breakable(
          for (eventInfo <- eventInfoItr) {
            if (eventInfo.evid == "coupon") {
              //判断领取优惠卷的uid
              uidSet.add(eventInfo.uid)
              itemSet.add(eventInfo.itemid)
            }
            if (eventInfo.itemid == "clickItem") {
              isClickItem = true
              break
            }
            eventList.add(eventInfo.evid)

          })
        //是否三次不同账号并没有浏览商品
        if (uidSet.size() >= 3 && isClickItem == false) {
          isAlert = true
        }
        (isAlert, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
      }
    }
    //只留true的，报警的
    val filteredAlertInfo: DStream[(Boolean, AlertInfo)] = alertInfoDStream.filter(_._1)
    //增加一个id 用于保存到es的时候进行去重操作
    val alertInfoWithIdDstream: DStream[(String, AlertInfo)] = filteredAlertInfo.map {
      case (flag, alertInfo) => {
        val period: Long = alertInfo.ts/60000
        val id: String = alertInfo.mid + "_" + period
        (id, alertInfo)
      }
    }
    alertInfoWithIdDstream.foreachRDD{rdd=>
      //以分区为单位 进行批量插入ES
      rdd.foreachPartition{ alertWithIdItr=>
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_COUPON_ALERT, alertWithIdItr.toList)
      }
    }
    alertInfoWithIdDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
