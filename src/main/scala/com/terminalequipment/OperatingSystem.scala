package com.luodesong.terminalequipment

import java.sql.{Connection, Statement}

import com.luodesong.util.{DBConnectionPool, LocationUtil, MakeAnsUtil, MakeTupeRddUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 终端设备
  *     操作系统
  *     inputPath：E:\Test-workspace\testSpark\output\project\DMP\parquet
  *     outputPath：end
  */
object OperatingSystem {

    def main(args: Array[String]): Unit = {
        // 模拟企业开发模式，首先判断一下目录 是否为空
        if (args.length != 2) {
            println("目录不正确，退出程序！")
            sys.exit()
        }
        // 创建一个集合，存储一下输入输出目录
        val Array(inputPath, outputPath) = args
        val conf = new SparkConf()
                .setAppName(this.getClass.getName).setMaster("local")
                // 处理数据，采取scala的序列化方式，性能比Java默认的高
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)
        // 我们要采取snappy压缩方式， 因为咱们现在用的是1.6版本的spark，到2.0以后呢，就是默认的了
        // 可以不需要配置
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")
        import sqlContext.implicits._
        val logsDF: DataFrame = sqlContext.read.parquet(inputPath)

        // 通过调用算子的方式处理数据
        /**
          * MakeTupeRddUtil.getTupes(logs:DataFrame, flagString: String)：这是自定义的方法用于拼接元组
          *     logs：读取进来的RDD或者是DataFrame
          *     flagString：这个是数据处理类型的标志，有了这个标志才能清楚拼接什么RDD
          *
          */
        val clientNameAndList: RDD[(String, List[Double])] = MakeTupeRddUtil.getTupes(logsDF,"client")

        //这一步是封装了一个聚合函数，传进去的是一个RDD
        val ansRDD: RDD[(String, List[Double])] = MakeAnsUtil.getAns(clientNameAndList)

        //装载数据到数据库
        ansRDD.foreachPartition(t => {
            val connection: Connection = DBConnectionPool.getConn()
            val statement: Statement = connection.createStatement()
            t.foreach(x => {
                val sql: String = s"insert into ClientNameInfo values('${x._1}',${x._2(0).toInt},${x._2(1).toInt}, ${x._2(2).toInt}, ${x._2(3).toInt}, ${x._2(4).toInt}, ${x._2(5).toInt}, ${x._2(6).toInt}, ${x._2(7)}, ${x._2(8)})"
                statement.execute(sql)
            })
            DBConnectionPool.releaseCon(connection)
        })

        //这一步是封装成一个DF
        val rowRDD: RDD[ClientNameInfo] = ansRDD.map(x => {
            ClientNameInfo(x._1, x._2(0).toInt, x._2(1) toInt, x._2(2).toInt, x._2(3).toInt, x._2(4).toInt, x._2(5).toInt, x._2(6).toInt, x._2(7), x._2(8))
        })
        val ansDF: DataFrame = rowRDD.toDF()
        ansDF.show()
    }
}
case class ClientNameInfo(clientName: String, originalRequest: Int, validRequest: Int, advertisingRequest: Int, showAmount: Int, clinkAmount: Int, parAmount: Int, accAmount: Int, endOne: Double, endTwo: Double)

