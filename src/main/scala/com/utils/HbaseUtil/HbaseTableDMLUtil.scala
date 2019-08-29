package com.utils.HbaseUtil

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}

object HbaseTableDMLUtil {

    def put(hbaseConnection: Connection, NSName: String, tableName: String, rowKey: String, cfName: String, colName: String, colValue: String): Unit = {
        //1.根据给定的NS名和表名创建table对象
        val table: Table = hbaseConnection.getTable(TableName.valueOf(NSName + ":" + tableName))
        //2.根据rowkey创建put对象
        val put = new Put(rowKey.getBytes())
        //3.往指定的rowkey中的列族中插入数据
        put.addColumn(cfName.getBytes, colName.getBytes, colValue.getBytes)
        //4.将上述put对象添加到表中 即：将指定rowkey、cfname、colname、colvalue的数据添加到表中
        table.put(put)
    }

}
