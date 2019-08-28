package com.luodesong.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * http请求协议
  */
object HttpUtil {

    //Get请求
    def getJson(url:String) : String = {
        //创建一个连接
        val client: CloseableHttpClient = HttpClients.createDefault()
        //通过传进来的一个url来获取连接
        val get: HttpGet = new HttpGet(url)
        //通过连接获取一个响应
        val response: CloseableHttpResponse = client.execute(get)
        //获取返回结果(是一个json的字符串儿)
        val jsonAns: String = EntityUtils.toString(response.getEntity)
        jsonAns
    }
}
