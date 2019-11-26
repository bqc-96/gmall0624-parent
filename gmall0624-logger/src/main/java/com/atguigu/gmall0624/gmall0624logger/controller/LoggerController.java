package com.atguigu.gmall0624.gmall0624logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0624.common.constant.GmallConstant;
import jdk.nashorn.internal.runtime.JSONFunctions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController//加上Rest不返回网页
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

  @PostMapping("log")//请求路径
public String log(@RequestParam("logString") String logString){//把logString参数的值注入变量中
      //补时间戳
      JSONObject jsonObject = JSON.parseObject(logString);//解析时间戳
      jsonObject.put("ts",System.currentTimeMillis());//加时间
      String jsonString = jsonObject.toJSONString();//再转成json
      //落盘成文件
      log.info(jsonString);
      //发送kafka
      //区分发送到不同的topic
      if(jsonObject.getString("type").equals("startup")){
          kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
      }else{
          kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
      }

      return "success";
}
}
