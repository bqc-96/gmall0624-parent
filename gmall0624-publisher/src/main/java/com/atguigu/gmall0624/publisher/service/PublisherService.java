package com.atguigu.gmall0624.publisher.service;

//数据业务查询接口

import java.util.List;
import java.util.Map;

public interface PublisherService {
    public Long getDauTotal(String date);
    public Map getDauHour(String date);
    //单日交易总额
    public Double getOrderAmount(String date);
    //交易额分时数
    public Map getOrderAmountHour(String date);
}
