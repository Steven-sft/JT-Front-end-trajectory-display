package com.ljj.toshow.demos.tools;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class totalOps {


    public static void getByRowkey(String tableName,String rowkey) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141");  // Zookeeper 地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // Zookeeper 端口
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("Row Key: " + Bytes.toString(result.getRow()));
                System.out.println("Column Family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("Column Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
        // 检查表是否存在的工具方法
    private static boolean isTableExists(Connection connection, String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }


    public static void createOrDis(Admin admin,String table,HTableDescriptor hTableDescriptor) throws IOException {
        if(admin.tableExists(TableName.valueOf(table))){
            System.out.println("Table already exists!");
        }else {
            admin.createTable(hTableDescriptor);
        }
    }
    public static void createTable(Configuration conf,String tableName,String cf) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Admin admin = connection.getAdmin()){
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(cf).setCompressionType(Compression.Algorithm.NONE));
            System.out.println("Creating table " + tableName+"...");
            createOrDis(admin,tableName,tableDescriptor);
            System.out.println("Done.");
        }
    }
    public static void deleteTable(Configuration conf,String tableName,String cf) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Admin admin = connection.getAdmin()){
            TableName table = TableName.valueOf(tableName);
            //停用表
            admin.disableTable(table);
            //删除列族
            admin.deleteColumn(table,cf.getBytes(StandardCharsets.UTF_8));
            //删除表
            admin.deleteTable(table);
        }
    }
    public static void adadColumnFamily(Configuration conf,String columnFamilyName,String tableName) throws IOException {
         try(Connection connection =ConnectionFactory.createConnection(conf) ;Admin admin = connection.getAdmin();){
             HColumnDescriptor hcd = new HColumnDescriptor(columnFamilyName);
             admin.addColumn(TableName.valueOf(tableName), hcd);
         }
    }
    public static void deleteByRowkey(Configuration conf,String columnFamilyName,String tableName) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Table table = connection.getTable(TableName.valueOf(tableName))){
            Delete delete = new Delete(Bytes.toBytes(columnFamilyName));
        }
    }
    public static void putLine(Configuration conf,String columnFamilyName,String tableName,String row1,String qualifier,String value) throws IOException {
        try(Connection connection =ConnectionFactory.createConnection(conf);Table table = connection.getTable(TableName.valueOf(tableName))){
            Put put = new Put(Bytes.toBytes(row1));//1001
            put.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(qualifier),Bytes.toBytes(value));
            System.out.println("one put succeed"+value);
            table.put(put);
        }
    }
   public static void putManyLines(Configuration conf,
                                String columnFamilyName,
                                String tableName,
                                String row1,
                                String qualifier,
                                String value,
                                String qualifier2,
                                String value2 ) throws IOException {
    try(Connection connection =ConnectionFactory.createConnection(conf);Table table = connection.getTable(TableName.valueOf(tableName))){
        Put put = new Put(Bytes.toBytes(row1));//1001
        put.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(qualifier),Bytes.toBytes(value)).addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(qualifier2),Bytes.toBytes(value2));
        System.out.println("two put succeed"+value+"     "+value2);
        table.put(put);
    }

}

}
