package com.atguigu.gmall0624.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0624.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    /**
     * 求各种总数
     * @param date
     * @return
     */
    @GetMapping("realtime-total")
public String getRealtimeTotal(@RequestParam("date") String date){
    Long dauTotal = publisherService.getDauTotal(date);//日活数
    List<Map> totalList = new ArrayList<>();

    Map dauMap = new HashMap();
    dauMap.put("id","dau");
    dauMap.put("name","新增日活");
    dauMap.put("value",dauTotal);
    totalList.add(dauMap);
    Map newMidMap = new HashMap();
    newMidMap.put("id","new_mid");
    newMidMap.put("name","新增设备");
    newMidMap.put("value",233);
    totalList.add(newMidMap);

        Map orderAmountMap = new HashMap();
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易总额");
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);
}

    /**
     * 求各种分时数
     * @param id
     * @param td
     * @return
     */
    @GetMapping("realtime-hour")
public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String td){
    if(id.equals("dau")){
        Map dauHourMapTD = publisherService.getDauHour(td);//今天数据
        //获取昨天时间
        String yd = getYD(td);
        Map dauHourMapYD = publisherService.getDauHour(yd);//昨天数据
        HashMap hourMap = new HashMap();
        hourMap.put("yesterday",dauHourMapYD);
        hourMap.put("today",dauHourMapTD);
        return JSON.toJSONString(hourMap);
    }else if(id.equals("order_amount")){
        Map orderAmountHourTD = publisherService.getOrderAmountHour(td);//今天数据
        //获取昨天时间
        String yd = getYD(td);
        //昨天数据
        Map orderAmountHourYD = publisherService.getOrderAmountHour(yd);
        Map amountMap = new HashMap();
        amountMap.put("yesterday",orderAmountHourYD);
        amountMap.put("today",orderAmountHourTD);
        return JSON.toJSONString(amountMap);
    }else{
        return null;
    }

}


//获取昨天时间
private String getYD(String td){
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
        //今天时间
        Date today = simpleDateFormat.parse(td);
        //昨天时间
        Date yesterday = DateUtils.addDays(today, -1);
        return simpleDateFormat.format(yesterday);
    } catch (ParseException e) {
        e.printStackTrace();
    }
return null;
}
}
