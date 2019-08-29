package com.tag


import com.utils.{JedisPool, TagUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 上下文标签
  * dericPath：E:\Test-workspace\testSpark\output\project\DMP\appAndIp
  * inputPath：E:\Test-workspace\testSpark\output\project\DMP\parquet
  * outputPath：E:\Test-workspace\testSpark\output\project\DMP\logs
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    // 模拟企业开发模式，首先判断一下目录 是否为空
    if (args.length != 3) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合，存储一下输入输出目录
    val Array(dericPath, inputPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local[*]")
      // 处理数据，采取scala的序列化方式，性能比Java默认的高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // 我们要采取snappy压缩方式， 因为咱们现在用的是1.6版本的spark，到2.0以后呢，就是默认的了
    // 可以不需要配置
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    //读取字典文件：用于广播查找只有appName的id的appName
    val dericRDD: RDD[String] = sc.textFile(dericPath)
    val temp: RDD[(String, String)] = dericRDD.map(x => {
      val str: String = x.substring(1, x.length - 1)
      val strings: Array[String] = str.split(",")
      //过滤掉“(,主题)”这种key为空值的数据
      if (strings.length == 2) {
        (strings(0) -> strings(1))
      } else {
        ("-1", "0")
      }
    })
    //广播变量字典文件
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(temp.collect().toMap)

    //读取的是过滤文件：用于过滤的是关键字，比如禁用词语
    val deric1RDD: RDD[String] = sc.textFile("E:\\Git\\input\\pro_ad\\stopwords.txt")
    //广播变量，关键字过滤
    val broadcast1: Broadcast[Array[String]] = sc.broadcast(deric1RDD.collect())

    //读取点击流日志
    val df: DataFrame = sqlContext.read.parquet(inputPath)
    val userIdAndTag: RDD[(String, List[(String, Int)])] = df.filter(TagUtil.oneUserId)
      .mapPartitions(temp => {
        val re: Jedis  = JedisPool.getMyrdis
        temp.map(row => {
          //取出用户id
          val userId: String = TagUtil.getOneUserid(row)
          //通过row数据打上所有标签(按需求)
          val advTag: ListBuffer[(String, Int)] = TagAdv.makeTags(row)
          //获取的是app的tag，传入的参数是row和广播出来的map值
          //val appNameTag: ListBuffer[(String, Int)] = TagApp.makeTags(row, broadcast.value)

          /**
            * 通过读取redis的数据字典
            */
          //获取的是app的tag，传入的参数是row和还有就是连接reids的连接
          val appNameTag: ListBuffer[(String, Int)] = TagAppRedis.makeTags(row, re)
          //获取渠道的标签
          val channleTag: ListBuffer[(String, Int)] = TagCha.makeTags(row)
          //获取设备的标签
          val equpmentTag: ListBuffer[(String, Int)] = TagEqu.makeTags(row)
          //获取关键字符的标签
          val keyTag: ListBuffer[(String, Int)] = TagKey.makeTags(row).filterNot( x => {
            broadcast1.value.contains(x)
          })
          //获取地理位置的标签
          val locationTag: ListBuffer[(String, Int)] = TagLoc.makeTags(row)
          //获取商圈标签
          val businessTag: ListBuffer[(String, Int)] = TagBusi.makeTags(row, re)

          /**
            * (userid，list(("LC03",1),("LC03",1),("CNxxxx",1),("App爱奇艺",1),("App爱奇艺",1),("D00010001",1),("D00010002",1)))
            */
          (userId, (advTag ++ appNameTag ++ channleTag ++ equpmentTag ++ keyTag ++ locationTag ++ businessTag).toList)
        })
      })
    //处理组合结果list
    /**
      * 先按照key分组聚合
      */
    val userIdAndTags: RDD[(String, List[(String, Int)])] = userIdAndTag.reduceByKey((list1, list2) => (list1 ::: list2))
    val ans: RDD[(String, List[(String, Int)])] = userIdAndTags.map(x => {
      /**
        * 在每个key的value值中在进行一次分组求和
        */
      val stringToInt: List[(String, Int)] = x._2.groupBy(_._1).mapValues(x => x.size).toList
      (x._1, stringToInt)
    })


    //将结果落地到磁盘
    ans.saveAsTextFile(outputPath)
    sc.stop()
  }

}
