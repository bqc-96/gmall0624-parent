package com.atguigu.gmall0624.publisher.service.impl;

import com.atguigu.gmall0624.publisher.mapper.DauMapper;
import com.atguigu.gmall0624.publisher.mapper.OrderMapper;
import com.atguigu.gmall0624.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//业务查询的实现类
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;
    @Autowired
    OrderMapper orderMapper;
    @Override
    public Long getDauTotal(String date) {
        Long dauTotal = dauMapper.getDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> dauHour = dauMapper.getDauHour(date);//[{"loghour":9,"ct":222}]
        Map dauHourMap = new HashMap();//{"9":222,}
        for (Map map : dauHour) {
            dauHourMap.put(map.get("loghour"),map.get("ct"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        Double orderAmounts = orderMapper.selectOrderAmount(date);
        return orderAmounts;
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        HashMap amountHourMap = new HashMap();
        for (Map map : mapList) {
        amountHourMap.put(map.get("create_hour"), map.get("order_amount"));
        }
        return amountHourMap;
    }
}
