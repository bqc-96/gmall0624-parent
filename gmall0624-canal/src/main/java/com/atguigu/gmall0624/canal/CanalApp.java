package com.atguigu.gmall0624.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {
    public static void main(String[] args) {
        //1 建立 和canal-server 连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        while (true) {
            //开始连接，这个连接不是持续的，一次一次连，所以需要循环
            canalConnector.connect();
            //2.订阅数据
            canalConnector.subscribe("*.*");
            //3.抓取 message n条sql
            Message message = canalConnector.get(100);
            //一个Entries相当于一条sql变化(sql,变化的数据)
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() == 0) {
                try {
                    System.out.println("没有数据，休息一会");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //遍历entries集合
                for (CanalEntry.Entry entry : entries) {
                    //进行判断，只要数据相关处理
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        //反序列化
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集，变化数据的集合
                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //获取操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();
                    }
                }
            }
        }

        //3.抓取message ,n条sql的数据的变化


    }
}
