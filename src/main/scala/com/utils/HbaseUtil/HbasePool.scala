package com.utils.HbaseUtil

import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbasePool {
    private val configValues = ConfigFactory.load("hbase.properties")
    private var conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", configValues.getString("hbase.zookeeper.quorum"))
    conf.set("hbase.zookeeper.property.clientPort", configValues.getString("hbase.zookeeper.property.clientPort"))
    conf.set("hbase.defaults.for.version.skip", configValues.getString("hbase.zookeeper.property.clientPort"))
    private val service: ExecutorService = Executors.newFixedThreadPool(configValues.getString("hbase.max_connections").toInt)
    private val conn: Connection = ConnectionFactory.createConnection(conf, service)

    /**
      * 获取连接池
      * @return
      */
    def getConnection: Connection = {
        conn
    }

    /**
      * 释放连接
      */
    def reseConnection(connection: Connection): Unit ={
        connection.close()
    }

}
