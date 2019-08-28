package com.tag


import com.tag.thetrait.Tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 位置的标签
  */
object TagLoc extends Tag {
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

        //组装元组类型的List
        val rtbprovince: String = row.getAs[String]("provincename")
        val rtbcity: String = row.getAs[String]("cityname")
        myList.append(("ZP" + rtbprovince, 1))
        myList.append(("ZC" + rtbcity, 1))
        myList
    }
}
