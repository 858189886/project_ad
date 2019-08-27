package com.app

import java.sql
import com.utils.{DBConnectionPool, LocationUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 媒体分析
  *
  *   爱奇艺 等
  *   dericpath:
  *   inputpath:
  *   outputpath: end 占位
  */
object AppSparkCore {
  def main(args: Array[String]): Unit = {
    //模拟企业开发模式,首先判断一下目录是否为空
    if(args.length != 3) {
      println("目录不正确,退出程序!")
      sys.exit()
    }
    //创建一个集合,存储输入输出目录
    val Array(dericpath, inputpath, outputpath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      //处理数据,采取scala的序列化方式,性能比Java默认的高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //要设置snappy压缩方式,因为 现版本是1.6的,到2.0以后就是默认采用snappy
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")
    import sqlContext.implicits._

    val dericRDD: RDD[String] = sc.textFile(dericpath)
    val temp: RDD[(String, String)] = dericRDD.map(x => {
      val str: String = x.substring(1, x.length - 1)
      val strings: Array[String] = str.split(",")
      //过滤掉“(,主题)”这种key为空值的数据
      if (strings.length == 2 ){
        (strings(0) -> strings(1))
      } else {
        ("-1", "0")
      }
    })
    //设置广播变量
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(temp.collect().toMap)

    val logsDF: DataFrame = sqlContext.read.parquet(inputpath)

    //处理数据
    val appNameAndList: RDD[(String, List[Double])] = logsDF.map(row => {
      // 先去获取需要的参数
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      var appname: String = row.getAs[String]("appname")
      val appid: String = row.getAs[String]("appid")

      //如果数据的appName为空的，就通过id去广播变量中去查
      if (appname.equals("") || appname.equals(" ")) {
        appname = broadcast.value.getOrElse(appid, "0")
      }

      //通过工具类求得每一组中的数据的情况，是1,0数组
      val reList: List[Double] = LocationUtil.doRequest(requestmode, processnode)
      val cliList: List[Double] = LocationUtil.doCli(requestmode, iseffective)
      val pricList: List[Double] = LocationUtil.doAdv(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      //组合tump
      (appname, reList ++ cliList ++ pricList)
    })

    //聚合数据
    val ansRDD: RDD[(String, List[Double])] = appNameAndList.reduceByKey((x, y) => {
      x.zip(y).map(t => {
        t._1 + t._2
      })
    })
    /**
      * 出数据到数据库中
      */
    //这个是在driver端执行的
    ansRDD.foreachPartition(pr => {
      val connection: sql.Connection =DBConnectionPool.getConn()
      val statement: sql.Statement = connection.createStatement()
      //这个是在exector端执行的 如果直接执行这个的会报一个没有序列化的错
      pr.foreach(x => {
        statement.execute(s"insert into AppNameInfo values('${x._1.toString}',${x._2(0).toInt},${x._2(1).toInt}, ${x._2(2).toInt}, ${x._2(3).toInt}, ${x._2(4).toInt}, ${x._2(5).toInt}, ${x._2(6).toInt}, ${x._2(7)}, ${x._2(8)})")
      })
      DBConnectionPool.releaseCon(connection)
    })

    //组装成一个DF
    val rowRDD: RDD[AppNameInfo] = ansRDD.map(x => {
      AppNameInfo(x._1, x._2(0).toInt, x._2(1) toInt, x._2(2).toInt, x._2(3).toInt, x._2(4).toInt, x._2(5).toInt, x._2(6).toInt, x._2(7), x._2(8))
    })
    val ansDF: DataFrame = rowRDD.toDF()
    ansDF.show()

    sc.stop()
  }
}
case class AppNameInfo(appname: String, originalRequest: Int, validRequest: Int, advertisingRequest: Int, showAmount: Int, clinkAmount: Int, parAmount: Int, accAmount: Int, endOne: Double, endTwo: Double)



