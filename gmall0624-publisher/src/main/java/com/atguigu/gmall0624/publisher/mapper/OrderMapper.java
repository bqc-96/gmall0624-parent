package com.atguigu.gmall0624.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //单日交易总额
    public Double selectOrderAmount(String date);
    //交易额分时数
    public List<Map> selectOrderAmountHour(String date);
}
