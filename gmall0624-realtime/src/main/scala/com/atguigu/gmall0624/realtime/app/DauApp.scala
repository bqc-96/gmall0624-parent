package com.atguigu.gmall0624.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.bean.StartUpLog
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.MykafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Dau_app")
    val ssc = new StreamingContext(conf,Seconds(5))
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)
//    inputDstream.foreachRDD(
//      rdd=>{
//        println(rdd.map(_.value()).collect().mkString("\n"))
//      }
//    )
    //去重
    //1.当日用户访问清单保存到redis中
    //2.利用redis进行过滤
    //redis type set,key dau:2019-11-26 value mid
    val startUPDstream: DStream[StartUpLog] = inputDstream.map {
      rdd => {
        val jsonString: String = rdd.value()
        //把json字符串转成json对象
        val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
        //补上时间戳
        val date = new Date(startUpLog.ts)
        val format = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateString: String = format.format(date)
        //转换时间格式
        val dateArr: Array[String] = dateString.split(" ")
        startUpLog.logDate = dateArr(0)
        startUpLog.logHour = dateArr(1)
        startUpLog
      }
    }

    //过滤
    //过滤重复的数据：同mid的数据；先到redis查出所有的数据,进行比较过滤
    val filterDStream: DStream[StartUpLog] = startUPDstream.transform { rdd =>
      ///  .....  driver 周期性的查询redis的清单   通过广播变量发送到executor中
      println("过滤前：" + rdd.count())
      val jedis = new Jedis("hadoop102", 6379) //连接redis，创建对象
    val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val dateKey: String = "dau:" + dateStr
      val dauMidSet: util.Set[String] = jedis.smembers(dateKey)
      jedis.close()
      //创建广播变量，发送到executor
      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)

      val filteredRdd: RDD[StartUpLog] = rdd.filter { startuplog => //executor 根据广播变量 比对 自己的数据  进行过滤
        val midSet: util.Set[String] = dauMidBC.value
        !midSet.contains(startuplog.mid)
      }
      println("过滤后：" + filteredRdd.count())
      filteredRdd
    }

    //批次内部去重，mid分组取第一个

    val startgroupDS: DStream[(String, Iterable[StartUpLog])] = filterDStream.map(
      startuplog => {
        (startuplog.mid, startuplog)
      }
    ).groupByKey()
    val flatDStream: DStream[StartUpLog] = startgroupDS.flatMap {
      case (mid, startItr) => {
        val sortList: List[StartUpLog] = startItr.toList.sortWith {
          (left, right) => {
            left.ts < right.ts
          }
        }
        val topLog: List[StartUpLog] = sortList.take(1)
        topLog
      }
    }


    flatDStream.foreachRDD{
      rdd=>{
        rdd.foreachPartition{
          startuplogItr=>{
            val jedis = new Jedis("hadoop102",6379)
            for (startuplog <- startuplogItr) {
              val dateKey="dau:"+startuplog.logDate
              //写入redis(dau:2019-11-26 ,mid)
              jedis.sadd(dateKey,startuplog.mid)
            }
            jedis.close()
          }
        }
      }
    }
    //把数据写入hbase+phoenix
    flatDStream.foreachRDD(rdd=>{
      println("zaizz")
      rdd.saveToPhoenix("GMALL2019_DAU",Seq("MID","UID","APPID","AREA","OS","CH","TYPE","VS","LOGDATE","LOGHOUR","TS"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
