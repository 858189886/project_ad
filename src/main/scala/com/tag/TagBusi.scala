package com.luodesong.tag

import ch.hsr.geohash.GeoHash
import com.luodesong.tag.thetrait.Tag
import com.luodesong.util.{JedisPool, MapUtil, Util2Type}
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 打商圈标签
  */
object TagBusi extends Tag{
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

        //解析参数1
        val row: Row = args(0).asInstanceOf[Row]

        //解析参数2
        val re: Jedis = args(1).asInstanceOf[Jedis]
        //获取经纬度
        val longs: Double = Util2Type.toDouble(row.getAs[String]("long"))
        val lat: Double = Util2Type.toDouble(row.getAs[String]("lat"))
        if (longs >= 73 && longs <= 135 && lat >= 3 && lat <= 54) {
            // 通过经纬度去缓存中获取
            val business: String = getBusiness(longs, lat, re)
            if ((!business.isEmpty) && (!business.equals(""))){
                val strings: Array[String] = business.split(",")
                for (k <- strings) {
                    if (k == "没有商业圈"){
                        myList.append((k, 0))
                    } else {
                        myList.append((k, 1))
                    }
                }
            }
        } else {
            myList.append(("未知区域", 0))
        }
        JedisPool.releaseMyredis(re)
        myList
    }

    /**
      * 获取商圈信息
      * @param longs
      * @param lat
      * @return
      */
    def getBusiness(longs:Double, lat:Double, re:Jedis): String = {
        //转换Geohash字符串
        val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, longs, 8)
        //数据库查询
        //判断商圈是否为空
        if(re.get(geohash) == null) {
            //通过经纬度获取商圈
            var busniss: String = MapUtil.getBusinessFromAmap(longs, lat)
            //将数据存入redis
            if(busniss != null || !busniss.equals("")) {
                re.set("dmp:business:" + geohash, busniss)
            }
            if (busniss == null || busniss.equals("")) {
                re.set("dmp:business:" + geohash, "没有商业圈")
                busniss = "没有商业圈"
            }
            busniss
        } else {
            re.get(geohash)
        }
    }

}
