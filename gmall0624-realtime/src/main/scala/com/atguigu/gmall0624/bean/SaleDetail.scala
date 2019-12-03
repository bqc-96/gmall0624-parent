package com.atguigu.gmall0624.bean

import java.text.SimpleDateFormat
import java.util.Date

case class SaleDetail(
                       var order_detail_id: String = null,
                       var order_id: String = null,
                       var order_status: String = null,
                       var create_time: String = null,
                       var user_id: String = null,
                       var sku_id: String = null,
                       var user_gender: String = null,
                       var user_age: Int = 0,
                       var user_level: String = null,
                       var sku_price: Double = 0D,
                       var sku_name: String = null,
                       var dt: String = null) {
  def this(orderLog: OrderLog, detailLog: DetailLog) {
    this
    mergeOrderLog(orderLog)
    mergeDetailLog(detailLog)
  }

  def mergeOrderLog(orderLog: OrderLog): Unit = {
    if (orderLog != null) {
      this.order_id = orderLog.id
      this.order_status = orderLog.order_status
      this.create_time = orderLog.create_time
      this.dt = orderLog.create_date
      this.user_id = orderLog.user_id
    }
  }

  def mergeDetailLog(detailLog: DetailLog): Unit = {
    if (detailLog != null) {
      this.order_detail_id = detailLog.id
      this.sku_id = detailLog.sku_id
      this.sku_name = detailLog.sku_name
      this.sku_price = detailLog.order_price.toDouble
    }

  }

  def mergeUserInfo(userInfo: UserInfo): Unit = {
    if (userInfo != null) {
      this.user_id = userInfo.id

      val formattor = new SimpleDateFormat("yyyy-MM-dd")
      val date: Date = formattor.parse(userInfo.birthday)
      val curTs: Long = System.currentTimeMillis()
      val betweenMs = curTs - date.getTime
      val age = betweenMs / 1000L / 60L / 60L / 24L / 365L

      this.user_age = age.toInt
      this.user_gender = userInfo.gender
      this.user_level = userInfo.user_level

    }
  }

}

