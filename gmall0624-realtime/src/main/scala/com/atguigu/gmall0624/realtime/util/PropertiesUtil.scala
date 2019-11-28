package com.atguigu.gmall0624.realtime.util

import java.io.InputStreamReader
import java.util.Properties

import scala.tools.nsc.interpreter.InputStream

object PropertiesUtil {
  def main(args: Array[String]): Unit = {
val properties: Properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))//根据key获取值
  }
  def load(propertieName:String) : Properties ={
    //Properties的对象是个kv类型的值，用于读取配置文件
    val properties = new Properties()
    properties.load(
      new InputStreamReader(
        Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName),//参数是要读取配置文件的名字
        "UTF-8"
      )
    )
    properties
  }
}
