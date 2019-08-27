package com.tag

import com.luodesong.tag.thetrait.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 渠道的标签
  */
object TagCha extends Tag{
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
        val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
        myList.append(("CN" + adplatformproviderid, 1))
        myList
    }
}
