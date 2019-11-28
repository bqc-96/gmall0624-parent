package com.atguigu.gmall0624.publisher.service.impl;

import com.atguigu.gmall0624.publisher.mapper.DauMapper;
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
}
