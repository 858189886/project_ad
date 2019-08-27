package com.tag

import com.luodesong.tag.thetrait.Tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * appName的标签
  */
object TagApp extends Tag{
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
        //将广播变量给拿进来
        val directory: Map[String,String] = args(1).asInstanceOf[Map[String,String]]

        //组装元组类型的List
        var appname: String = row.getAs[String]("appname")
        val appid: String = row.getAs[String]("appid")
        if (appname.equals("") || appname.equals(" ")) {
            appname = directory.getOrElse(appid, "0")
        }
        //返回这个list
        myList.append(("APP " + appname, 1))
        myList
    }
}
