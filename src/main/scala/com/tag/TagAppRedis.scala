package com.tag

import com.tag.thetrait.Tag
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * appName的标签
  */
object TagAppRedis extends Tag{
    /**
      * 统一标签的方法
      */
    override def makeTags(args: Any*): ListBuffer[(String, Int)] = {
        //存放结果集的list，里面是一个元组
        /**
          * key：名称
          * value：1，表示出现过一次
          */
        val myList: ListBuffer[(String, Int)] = ListBuffer[(String, Int)]()

        //解析参数
        val row: Row = args(0).asInstanceOf[Row]
        val jedis: Jedis = args(1).asInstanceOf[Jedis]
        //获取前面传过来的jedis连接，用于连接redis通过key(ip)来查找value(appName)
        var appname: String = row.getAs[String]("appname")
        val appid: String = row.getAs[String]("appid")
        if (appname.equals("") || appname.equals(" ")) {
            //从redis中获得value值
            appname = jedis.get(appid)
        }
        myList.append(("APP " + appname, 1))
        myList
    }
}
