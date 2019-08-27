package com.ProCityCt

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 判断个省市数据量分布情况
  * inputpath:
  * outputpath:
  */
object Data2Json {
  def main(args: Array[String]): Unit = {
    //判断路径
    if (args.length != 2){
      println("目录参数不正确,退出程序")
      sys.exit()
    }
    //创建一个集合保存输入输出产数
    val Array(inputpath,outputpath) = args
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    //设置压缩方式
    sqlContext.setConf("spark.io.compression.codec","snappy")

    //读取parquet文件
    val logsDF: DataFrame = sqlContext.read.parquet(inputpath)

    //创建临时文件
    logsDF.registerTempTable("logs")

    val sqlString: String = "select count(*) ct,provincename,cityname from logss group byprovincename," +
      "cityname sort by ct desc "
    val ansDF: DataFrame = sqlContext.sql(sqlString)
    ansDF.show()

    /**
      * coalesce(1):减少分区的分区器
      */
    ansDF.coalesce(1).write.mode(SaveMode.Append).json(outputpath)

    sc.stop()


  }

}
