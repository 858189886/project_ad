package com.tag


import com.tag.thetrait.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer
/**
  * 广告标签
  */
object TagAdv extends Tag{
    /**
      * 统一标签的方法
      */
    override def makeTags(args: Any*):ListBuffer[(String, Int)] = {

        //存放结果集的list，里面是一个元组
        /**
          * key：名称
          * value：1，表示出现过一次
          */
        val myList: ListBuffer[(String, Int)] = ListBuffer[(String, Int)]()

        //解析参数
        val row: Row = args(0).asInstanceOf[Row]
        val adType: Int = row.getAs[Int]("adspacetype")
        //模式匹配组装元组类型的List
        adType match {
            case v if v > 9 => myList :+ ("LC" + v, 1)
            case v if v <= 9 && v > 0 => myList.append(("LC0" + v, 1))
        }
        val adName: String = row.getAs[String]("adspacetypename")
        if (StringUtils.isAnyBlank(adName)) {
            myList.append(("LN" + adName, 1))
        }
        //返回这个list
        myList
    }

}
