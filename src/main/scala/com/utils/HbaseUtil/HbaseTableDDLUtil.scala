package com.utils.HbaseUtil

import java.util.{ArrayList, List}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._


/**
  * hbase的表的操作
  */
object HbaseTableDDLUtil {

    /**
      * 创建一个HbaseTbable
      * @param conn 连接
      * @param NSName 命名空间
      * @param tableName 表名
      * @param cfNames 列族名
      */
    def createTable(conn: Connection, NSName: String, tableName: String, cfNames: String*): Unit = {
        //2.获取HbaseAdmin对象
        val hbaseAdmin: Admin = conn.getAdmin
        if(!hbaseAdmin.tableExists(TableName.valueOf(NSName + ":" + tableName))){
            //3.构造一个表描述符构造器对象
            val tdb: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(NSName + ":" + tableName))
            //4.根据传进来的多个列族名构造多个列族描述符对象
            val cfs: List[ColumnFamilyDescriptor] = new ArrayList[ColumnFamilyDescriptor]
            for (cfName <- cfNames) { //通过 构造列族描述符构造器 用其 构造列族描述符对象
                cfs.add(ColumnFamilyDescriptorBuilder.newBuilder(cfName.getBytes).build)
            }
            //5.将列族对象添加到表描述符构造器上
            tdb.setColumnFamilies(cfs)
            //6.使用表描述符构造器 构造表
            val table: TableDescriptor = tdb.build
            //7.根据表描述符对象创建表
            hbaseAdmin.createTable(table)
            System.out.println(NSName + ":" + tableName + "表创建完成")
            hbaseAdmin.close()
            conn.close()
        }
    }
}
