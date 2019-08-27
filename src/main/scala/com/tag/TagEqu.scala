package com.tag

import com.luodesong.tag.thetrait.Tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 设备
  */
object TagEqu extends Tag {
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

        val row: Row = args(0).asInstanceOf[Row]
        /**
          * client android
          * networkmannername 4G
          * ispname 联通
          */
        val client: Int = row.getAs[Int]("client")
        val networkmannername: String = row.getAs[String]("networkmannername")
        val ispname: String = row.getAs[String]("ispname")
        //模式匹配组装元组类型的List
        client match {
            case v if (v == 1) => myList.append(("D0001000" + 1, 1))
            case v if (v == 2) => myList.append(("D0001000" + 2, 1))
            case v if (v == 3) => myList.append(("D0001000" + 3, 1))
            case _ => myList.append(("D0001000" + 4, 1))
        }
        networkmannername match {
            case v if (v.equals("Wifi")) => myList.append(("D0002000" + 1, 1))
            case v if (v.equals("4G")) => myList.append(("D0002000" + 2, 1))
            case v if (v.equals("3G")) => myList.append(("D0002000" + 3, 1))
            case v if (v.equals("2G")) => myList.append(("D0002000" + 4, 1))
            case _ => myList.append(("D0002000" + 5, 1))

        }
        ispname match {
            case v if (v.equals("移动")) => myList.append(("D0003000" + 1, 1))
            case v if (v.equals("联通")) => myList.append(("D0003000" + 2, 1))
            case v if (v.equals("电信")) => myList.append(("D0003000" + 3, 1))
            case _ => myList.append(("D0003000" + 4, 1))
        }
        myList
    }
}
