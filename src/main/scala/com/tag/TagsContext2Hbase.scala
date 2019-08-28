package com.tag


import com.utils.HbaseUtil.Data2HbaseUtil
import com.utils.{JedisPool, TagUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object TagsContext2Hbase {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName(this.getClass.getName).setMaster("local[*]")
                // 处理数据，采取scala的序列化方式，性能比Java默认的高
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val sc = new SparkContext(conf)

        // 我们要采取snappy压缩方式， 因为咱们现在用的是1.6版本的spark，到2.0以后呢，就是默认的了
        // 可以不需要配置
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

        //读取的是过滤文件：用于过滤的是关键字，比如禁用词语
        val deric1RDD: RDD[String] = sc.textFile("E:\\Test-workspace\\testSpark\\input\\project\\DMP\\keyFilter")
        //广播变量，关键字过滤
        val broadcast1: Broadcast[Array[String]] = sc.broadcast(deric1RDD.collect())

        //读取点击流日志
        val df: DataFrame = sqlContext.read.parquet("E:\\Test-workspace\\testSpark\\output\\project\\DMP\\parquet_less")
        val useridsAndRowRDD: RDD[(List[String], Row)] = df.filter(TagUtil.oneUserId).map(row => {
            val userIds: List[String] = TagUtil.getAnyAllUserId(row)
            (userIds, row)
        })

        //构建点集合
        /**
          * 使用flatMap的原因：
          * if (tp._1.head.equals(uId)) {
          * (uId.hashCode, VD)
          * } else {
          * (uId.hashCode, List.empty)
          * }
          * 这一步会产生多个tupe，必须得将多个tupe压平
          */
        val pointRDD: RDD[(Long, List[(String, Int)])] = useridsAndRowRDD.mapPartitions(x => {
            val re: Jedis = JedisPool.getMyrdis
            x.map(tp => {
                //通过row数据打上所有标签(按需求)
                val advTag: ListBuffer[(String, Int)] = TagAdv.makeTags(tp._2)
                //获取的是app的tag，传入的参数是row和还有就是连接reids的连接
                val appNameTag: ListBuffer[(String, Int)] = TagAppRedis.makeTags(tp._2, re)
                //获取渠道的标签
                val channleTag: ListBuffer[(String, Int)] = TagCha.makeTags(tp._2)
                //获取设备的标签
                val equpmentTag: ListBuffer[(String, Int)] = TagEqu.makeTags(tp._2)
                //获取关键字符的标签
                val keyTag: ListBuffer[(String, Int)] = TagKey.makeTags(tp._2).filterNot(x => {
                    broadcast1.value.contains(x)
                })
                //获取地理位置的标签
                val locationTag: ListBuffer[(String, Int)] = TagLoc.makeTags(tp._2)
                //获取商圈标签
                val businessTag: ListBuffer[(String, Int)] = TagBusi.makeTags(tp._2, re)

                val tuples: List[(String, Int)] = (advTag ++ appNameTag ++ channleTag ++ equpmentTag ++ keyTag ++ locationTag ++ businessTag).toList
                val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ tuples
                tp._1.map(uId => {
                    if (tp._1.head.equals(uId)) {
                        (uId.hashCode.toLong, VD)
                    } else {
                        (uId.hashCode.toLong, List.empty)
                    }
                })
            })
        }).flatMap(x => x)

        //构建边集合
        /**
          * Edge(arg1, arg2, 0)
          * arg1：是起点
          * arg2：是终点
          * 0：权值
          */
        val edges: RDD[Edge[Int]] = useridsAndRowRDD.map(x => {
            x._1.map(tp => {
                Edge(x._1.head.hashCode.toLong, tp.hashCode.toLong, 0)
            })
        }).flatMap(x => x)

        val days: String = "2017-01-08"

        //构建图
        val graph: Graph[List[(String, Int)], Int] = Graph(pointRDD, edges)

        //找到该图的最大节点
        val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

        //连接最大节点的tags
        val joinEd: RDD[(VertexId, (VertexId, List[(String, Int)]))] = vertices.join(pointRDD)

        //将tags的数据进行聚合
        val vertexIdAndTags: RDD[(VertexId, List[(String, Int)])] = joinEd.map(x => {
            (x._2)
        })
        val userIdAndTags: RDD[(VertexId, List[(String, Int)])] = vertexIdAndTags.reduceByKey(_ ++ _).map(x => {
            val list: List[(String, Int)] = x._2.groupBy(_._1).mapValues(x => x.size).toList
            (x._1, list)
        })

        //创建存入数据库的方式
        val ans: RDD[(ImmutableBytesWritable, Put)] = userIdAndTags.map(t => {
            val put = new Put(Bytes.toBytes(t._1))
            val tags: List[(String, Int)] = t._2
            val tagStr: String = tags.map(x => {
                x._1 + ":" + x._2
            }).mkString(",")
            put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$days"), Bytes.toBytes(tagStr))
            (new ImmutableBytesWritable(), put)
        })

        //测试的时候显示
        val ans1 = userIdAndTags.map(t => {
            val put = new Put(Bytes.toBytes(t._1))
            val tags: List[(String, Int)] = t._2
            val tagStr: String = tags.map(x => {
                x._1 + ":" + x._2
            }).mkString(",")
            (t._1, tagStr)
        })
        val jobconf: JobConf = Data2HbaseUtil.doData2Hbase(sc, "spark_dmp", "user_tags", "tags")
        ans.saveAsHadoopDataset(jobconf)
        ans1.foreach(println)
        sc.stop()
    }
}
