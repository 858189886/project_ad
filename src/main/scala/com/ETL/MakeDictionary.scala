package com.ETL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 这个方法是为了构建appname应用名称的数据字典,方便于做广播变量
  * 洗出来的数据形式如下:
  * (com.yh.daissd,1018代驾客户端
  *    第一个字段也就是key的值即appname的id
  *    第二个字段就是value的值即appname的name
  *
  *    做广播变量的目的是有的appname有的时候是空的,必须通过id来在字典变量中查找出来
  *    inputpath:
  *    outputpath:
  */
object MakeDictionary {
  def main(args: Array[String]): Unit = {
    //判断路径
    if(args.length != 2){
      println("目录参数不正确,退出程序")
      sys.exit()
    }
    //创建一个集合保存输入输出产数
    val Array (inputpath,outputpatth) = args
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val dictionaryLogs: RDD[String] = sc.textFile(inputpath)

    val tempRDD: RDD[Array[String]] = dictionaryLogs.map(x => {
      x.split("\t")     // "\\s"切割包括 空格和\t 的数据
    }).filter(_.length >= 5) //如果数据小于5的时候是没用的垃圾数据
    val appAndIdRDD: RDD[(String, String)] = tempRDD.map(x => {
      (x(4), x(1))
    })

    //coalesce(1)将分区数设置为一个
    //saveAsTextFile(outputpatth) 保存出去
    appAndIdRDD.coalesce(1).saveAsTextFile(outputpatth)
    sc.stop()

  }
}
