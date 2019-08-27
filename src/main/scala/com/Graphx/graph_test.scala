package com.Graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object graph_test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*}")
    val sc: SparkContext = new SparkContext(conf)
    //构造点的集合
    val vertexRDD = sc.makeRDD(Seq(
      (1L, ("詹姆斯", 35)),
      (2L, ("霍华德", 34)),
      (6L, ("杜兰特", 31)),
      (9L, ("库里", 30)),
      (133L, ("哈登", 30)),
      (138L, ("席尔瓦", 36)),
      (16L, ("法尔考", 35)),
      (44L, ("内马尔", 27)),
      (21L, ("J罗", 28)),
      (5L, ("高斯林", 60)),
      (7L, ("奥德斯基", 55)),
      (158L, ("马云", 55))
    ))
    //构造边的集合
     val edge: RDD[Edge[Int]] = sc.makeRDD(Seq(
       Edge(1L, 133L, 0),
       Edge(2L, 133L, 0),
       Edge(6L, 133L, 0),
       Edge(9L, 133L, 0),
       Edge(6L, 138L, 0),
       Edge(16L, 138L, 0),
       Edge(44L, 138L, 0),
       Edge(21L, 138L, 0),
       Edge(5L, 158L, 0),
       Edge(17L, 158L, 0)

     ))
    //构造图
    val graph = Graph(vertexRDD,edge)

  }
}
