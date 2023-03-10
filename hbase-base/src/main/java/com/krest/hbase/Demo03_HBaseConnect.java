package com.krest.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


import java.io.IOException;

public class Demo03_HBaseConnect {

    public static void main(String[] args) throws IOException {
        // 1.获取配置类
        Configuration conf = HBaseConfiguration.create();
        // 2.给配置类添加配置

        conf.set("hbase.zookeeper.quorum", "hadoop100,hadoop101,hadoop102");
        // 3.获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 3.获取 admin
        Admin admin = connection.getAdmin();
        // 5.获取 descriptor 的 builder
        TableDescriptorBuilder builder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf("bigdata", "staff4"));
        // 6. 添加列族
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build());
        // 7.创建对应的切分
        byte[][] splits = new byte[3][];
        splits[0] = Bytes.toBytes("aaa");
        splits[1] = Bytes.toBytes("bbb");
        splits[2] = Bytes.toBytes("ccc");
        // 8.创建表
        admin.createTable(builder.build(), splits);
        // 9.关闭资源
        admin.close();
        connection.close();
    }
}
