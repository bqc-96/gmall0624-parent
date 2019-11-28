package com.atguigu.gmall0624.publisher.mapper;

import java.util.List;
import java.util.Map;

//数据层查询的interface
public interface DauMapper {
    //日活跃总数接口
    public Long getDauTotal(String date);
    //日活分时数
public List<Map> getDauHour(String date);
}
