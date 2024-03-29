package com.atguigu.gmall0624.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0624.canal.util.MyKafkaSender;
import com.atguigu.gmall0624.common.constant.GmallConstant;

import java.util.List;

//canal业务
public class CanalHandler {
    CanalEntry.EventType eventType;
    String tableName;
    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }
    public void handle(){
        //表名为订单，类型是w1插入，说明应该是下单信息
        if (tableName.equals("order_info")&&eventType.equals(CanalEntry.EventType.INSERT)){
            rowDateList2Kafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }else if(tableName.equals("order_detail")&&eventType.equals(CanalEntry.EventType.INSERT)){
            rowDateList2Kafka(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
        }else if(tableName.equals("user_info")&&(eventType.equals(CanalEntry.EventType.INSERT)||eventType.equals(CanalEntry.EventType.UPDATE))){
            rowDateList2Kafka(GmallConstant.KAFKA_TOPIC_USER_INFO);
        }
    }
    private void  rowDateList2Kafka(String topic){
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();//改变后信息
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "---->" + column.getValue());
                jsonObject.put(column.getName(),column.getValue());
            }
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            MyKafkaSender.send(topic,jsonObject.toJSONString());
        }
    }
}