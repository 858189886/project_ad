package com.utils.HbaseUtil

import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.client.Connection

object HbaseNsUtil {

    /**
      * 创建一个namespace
      *
      * @param nsName : 命名空间名称
      */
    def createNamespace(conn:Connection,nsName: String): Unit = {

        //2.获取HbaseAdmin对象
        val hbaseAdmin = conn.getAdmin
        //3.创建hbaseNamespace描述对象
        val namespaceDescriptor: NamespaceDescriptor = NamespaceDescriptor.create(nsName).build()
        //4.使用hbaseAdmin对象根据hbaseNamespace描述对象创建一个hbaseNamespace
        hbaseAdmin.createNamespace(namespaceDescriptor)
        System.out.println("命名空间: " + nsName + "创建完成！");
    }

    /**
      * 获取所有的命名空间
      */
    def getAllNamespace(conn:Connection): Unit = {
        //2.获取HbaseAdmin对象
        val hbaseAdmin = conn.getAdmin
        //3.获取命令空间
        val namespaceDescriptors = hbaseAdmin.listNamespaceDescriptors
        //4.获取所有的列表
        for (namespaceDescriptor <- namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName)
        }
        System.out.println(namespaceDescriptors.length + " row(s)")
    }
}
