package com.atguigu.gmall0624.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_RORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return
    */
  def getClient: JestClient = {
    if (factory == null) {
      build()
    }
    factory.getObject
  }

  /**
    * 关闭客户端
    *
    * @param client
    */
  def close(client: JestClient): Unit = {
    if (client != null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_RORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build
    )
  }

//  //插入一条索引
//  def main(args: Array[String]): Unit = {
//    val jest: JestClient = getClient
//    //any:Map
//    //Index指插入新索引
////    val index: Index = new Index.Builder(Stu(1, "zhang3", 624)).index("stu0624").`type`("stu").id("1").build()
//    ////    jest.execute(index) //执行
//    ////    close(jest)
////    val tuples = List(("2",(2,"lisi",333)),("3",(3,"wangeu",666)),("4",(4,"gaorong",777)))
//  //insertBulk(tuples)
//  }

  //插入多条
  def insertBulk(indexName:String,list: List[(String, Any)]): Unit = {
    val jest: JestClient = getClient
    val bulkBulider = new Bulk.Builder()
    for ((id,elem)<- list) {
      val index: Index = new Index.Builder(elem).index(indexName).`type`("_doc").id(id).build()
    bulkBulider.addAction(index)
    }
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBulider.build()).getItems
    close(jest)
    println("保存" + items.size() + "条数据")
  }

  case class Stu(id: Int, name: String, classId: Int);
}

