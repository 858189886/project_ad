package com.luodesong.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer
/**
  * 高德地图解析
  */
object MapUtil {

    //获取高德地图的信息
    /**
      *
      * @param lon: 经度
      * @param lat：纬度
      * @return
      */
    def getBusinessFromAmap(lon: Double, lat: Double): String ={
        //拼接传进来的经纬度
        var location: String = lon + "," + lat
        //拼接http请求
        val url = s"https://restapi.amap.com/v3/geocode/regeo?location=${location}&key=a580937f8ea46ff8f9839691663805a4"
        //调用编写的httpHtil获取到json字符串
        val jsonString: String = HttpUtil.getJson(url)
        //调用fastjson的json的对象
        val json: JSONObject = JSON.parseObject(jsonString)

        //判断字符串的状态
        /**
          * status为0表示失败
          * status为1表示成功
          */
        val status: Int = json.getIntValue("status")
        status match {
            case v if (v == 0) => null
            case _ => {
                //获取第一层
                val regeocodeJson: JSONObject = json.getJSONObject("regeocode")
                regeocodeJson match {
                    case v if (v == null || regeocodeJson.keySet().isEmpty) => null
                    case _ => {
                        //获取第二层
                        val addressComponentJson: JSONObject = regeocodeJson.getJSONObject("addressComponent")
                        addressComponentJson match {
                            case v if (v == null || addressComponentJson.keySet().isEmpty) => null
                            case _ => {
                                //获取第最后层：这一层是一个数组
                                val businiessArr: JSONArray = addressComponentJson.getJSONArray("businessAreas")
                                businiessArr match {
                                    case v if(businiessArr == null || businiessArr.isEmpty) => null
                                    case _ => {
                                        val buffer: ListBuffer[String] = ListBuffer[String]()
                                        for (k <- businiessArr.toArray()) {
                                            if(k.isInstanceOf[JSONObject]){
                                                //获取相应的字段
                                                val js: JSONObject = k.asInstanceOf[JSONObject]
                                                val str: String = js.getString("name")
                                                buffer.append(str)
                                            }
                                        }
                                        //返回
                                        buffer.mkString(",")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
