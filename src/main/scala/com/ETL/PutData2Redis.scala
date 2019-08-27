package com.ETL

import com.utils.JedisPool
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object PutData2Redis {
  def main(args: Array[String]): Unit = {
    //判断路径
    if (args.length != 2) {
      println("目录参数不正确,退出程序")
      sys.exit()
    }
    //创建一个集合保存输入输出产数
    val Array(inputpath) = args
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val theDictionarys: RDD[String] = sc.textFile(inputpath)

    //通过连接池得到链接,对每个分区的数据进行数据存入操作
    theDictionarys.foreachPartition(x => {
      val jedis: Jedis = JedisPool.getMyrdis

      //对每一条数据进行操作
      x.foreach(x => {
        //切割并且处理数据
        val str: String = x.substring(1, x.length - 1)
        val strings: Array[String] = str.split(",")
        //过滤掉“(,主题)”这种key为空值的数据
        if (strings.length == 2) {
          //存入String类型的value值
          /**
            * key：是appName的Id
            * value：是appName
            */
          jedis.set("dmp:direc:" + strings(0), strings(1))
        } else {
          jedis.set("-1", "0")
        }
      })
      //关闭连接
      JedisPool.releaaseMyredis(jedis)
    })
  }
}
