package com.utils

import org.apache.spark.rdd.RDD

/**
  *
  * 处理的的是一个结果数据集合
  *     没有用key分组的时候： ((one,List(1,2,3,4,5),List(1,2,3,4,5),List(1,2,3,4,5)))
  *     用key分组了之后：(List(1,2,3,4,5),List(1,2,3,4,5),List(1,2,3,4,5))
  *     将他们zip后：((1,1),(2,2),(3,3),(4,4),(5,5))
  */
object MakeAnsUtil {
  def getAns(rdd:RDD[(String, List[Double])]):RDD[(String, List[Double])] ={
    rdd.reduceByKey((x, y) => {
      //将他们zip后：((1,1),(2,2),(3,3),(4,4),(5,5))
      x.zip(y).map(t => {
        //这一步是将每个tupe的两个元素加起来形成一个新的list如此循环
        t._1 + t._2
      })
    })
  }
}
