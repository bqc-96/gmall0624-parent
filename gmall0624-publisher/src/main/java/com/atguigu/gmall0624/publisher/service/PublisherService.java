package com.atguigu.gmall0624.publisher.service;

//数据业务查询接口

import java.util.Map;

public interface PublisherService {
    public Long getDauTotal(String date);
    public Map getDauHour(String date);
}
