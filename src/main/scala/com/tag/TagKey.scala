package com.tag

import com.luodesong.tag.thetrait.Tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object TagKey extends Tag {
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
        val keywords: String = row.getAs[String]("keywords")
        val strings: Array[String] = keywords.split("\\|")
        //组装元组类型的List
        for (k <- strings) {
            if (k.length >= 3 && k.length <= 8)
            myList.append(("K" + k, 1))
        }
        myList
    }
}
