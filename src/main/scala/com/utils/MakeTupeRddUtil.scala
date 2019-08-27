package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * 这一步的作用是组合成一个tupe的rdd
  *      因为有9个指标,需要的是按照省份和城市的分组将9个指标进行聚合
  *      所以会形成小面这种形式的每个数据才行
  *         (key,list(1,1,1,0,0,1,0,1,1)
  *      每次循环生成这样的数据,然后通过后面的聚合就能够求出结果
  */
object MakeTupeRddUtil {
  def getTupes(logs: DataFrame, flagString: String): RDD[(String, List[Double])] = {
    logs.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //通过工具类求得每一组中的数据的情况，是1,0数组 拼接了每一个元组的第二个元素
      val reList: List[Double] = LocationUtil.doRequest(requestmode, processnode)
      val cliList: List[Double] = LocationUtil.doCli(requestmode, iseffective)
      val pricList: List[Double] = LocationUtil.doAdv(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      val lists: List[Double] = reList ++ cliList ++ pricList

      //拼接每个元组的第一个元素
      var tupesKey: String = null
      if (flagString.equals("devicetype")) {
        val devicetype: Int = row.getAs[Int](flagString)
        var deviceName: String = null
        if (devicetype == 1) {
          deviceName = "手机"
        } else if (devicetype == 2) {
          deviceName = "平板"
        } else {
          deviceName = "其他"
        }
        tupesKey = deviceName
      }
      if (flagString.equals("networkmannername")) {
        tupesKey = row.getAs[String]("networkmannername")
      }
      if (flagString.equals("client")) {
        val client: Int = row.getAs[Int]("client")
        var clientName: String = null
        if (client == 1) {
          clientName = "android"
        } else if (client == 2) {
          clientName = "ios"
        } else if (client == 3) {
          clientName = "wp"
        } else {
          clientName = "其他"
        }
        tupesKey = clientName
      }
      if (flagString.equals("ispname")) {
        tupesKey = row.getAs[String]("ispname")
      }

      if (flagString.equals("location")) {
        val provincename: String = row.getAs[String]("provincename")
        val cityname: String = row.getAs[String]("cityname")
        tupesKey = provincename + ":" + cityname
      }

      //返回固定的元组
      (tupesKey, lists)
    })
  }

}