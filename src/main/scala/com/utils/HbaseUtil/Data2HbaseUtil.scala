package com.utils.HbaseUtil

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext

object Data2HbaseUtil {

    def doData2Hbase(sc:SparkContext, hbaseNamespace:String, hbaseTableName:String, cfName:String): JobConf = {
        //加载配置文件
        val load: Config = ConfigFactory.load("hbase.properties")

        // 创建Hadoop任务
        val configuration = sc.hadoopConfiguration
        configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))

        // 创建HbaseConnection
        val hbconn = ConnectionFactory.createConnection(configuration)

        // 如果表不存在的时候再创建表
        HbaseTableDDLUtil.createTable(hbconn,hbaseNamespace,hbaseTableName,cfName)

        // 创建JobConf
        val jobconf = new JobConf(configuration)
        // 指定输出类型和表
        jobconf.setOutputFormat(classOf[TableOutputFormat])
        jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseNamespace + ":" + hbaseTableName)
        jobconf
    }

}
