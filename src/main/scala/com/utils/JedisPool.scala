package com.utils

/**
  * jedis连接池
  *
  */

import java.util.LinkedList

import com.typesafe.config.ConfigFactory
import redis.clients.jedis.Jedis

/**
  * 如果直接使用JedisPool的话会报一个JedisPool没有序列化的异常。
  *     解决方法：
  *          1.自己封装连接池
  *          2.将连接池拿出来用一个object的简单类包装一下连接池中的连接
  */

object JedisPool {

  //读取 .properties配置文件
  private val config = ConfigFactory.load("redisPool.properties")
  private val max_connection = config.getString("redis.max_connection") //连接池总数
  private val connection_num = config.getString("redis.connection_num") //产生连接池
  private var current_num = 0 //当前连接池已产生的链接数
  private val pools = new LinkedList[Jedis]() //连接池
  private val node: String = config.getString("redis.node") //获取节点名称
  private val port: String = config.getString("redis.port") //获取节点端口

  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty) {
      println("busyness")
      Thread.sleep(2000)
      before()
    } else {
      print("创建连接")
    }
  }

  /**
    * 获得连接
    */
  private def initJedis(): Jedis = {
    new Jedis(node, port.toInt)
  }

  /**
    * 初始化连接池
    */
  private def initJedisPool(): LinkedList[Jedis] = {
    AnyRef.synchronized({
      if (pools.isEmpty) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initJedis())
          current_num += 1
        }
      }
      pools
    })
  }

  /**
    * 获得连接
    */
   def getMyrdis: Jedis = {
    initJedisPool()
    pools.poll()
  }

  /**
    * 释放连接
    */
  def releaaseMyredis(con:Jedis){
    pools.push(con)
  }
}
