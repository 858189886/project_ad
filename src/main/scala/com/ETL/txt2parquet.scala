package com.ETL

import com.utils.{SchemaUtil, Util2Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * txt格式的文件清洗转化为spark的默认parquet格式
  *
  * parquet是一个列式存储格式
  * inputpath:输入路径
  * outputpath:输出路径
  */

object txt2parquet {

  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if (args.length != 2) {
      println("目录参数不正确,退出程序")
      sys.exit()
    }

    //创建一个集合保存输入输出产数
    val Array(inputpath,outputpath) = args
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    //设置压缩方式
    sqlContext.setConf("spark.sql.parquet.compression.code","snappy")

    //进行数据的读取,处理分析数据
    val lines: RDD[String] = sc.textFile(inputpath)

    //按要求切割数据,并且保证数据的长度大于等于85,
    // -1的作用是保证相同切割条件的数据的重复,
    // 加上了-1才能保证数据的准确性: , , , , 会被当成一个字符切割
    val logsRDD: RDD[Array[String]] = lines.map(_.split(",",-1)).filter(_.length >= 85)

    val rowRDD: RDD[Row] = logsRDD.map(arr => {
      Row(
        arr(0),
        Util2Type.toInt(arr(1)),
        Util2Type.toInt(arr(2)),
        Util2Type.toInt(arr(3)),
        Util2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        Util2Type.toInt(arr(7)),
        Util2Type.toInt(arr(8)),
        Util2Type.toDouble(arr(9)),
        Util2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        Util2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        Util2Type.toInt(arr(20)),
        Util2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        Util2Type.toInt(arr(26)),
        arr(27),
        Util2Type.toInt(arr(28)),
        arr(29),
        Util2Type.toInt(arr(30)),
        Util2Type.toInt(arr(31)),
        Util2Type.toInt(arr(32)),
        arr(33),
        Util2Type.toInt(arr(34)),
        Util2Type.toInt(arr(35)),
        Util2Type.toInt(arr(36)),
        arr(37),
        Util2Type.toInt(arr(38)),
        Util2Type.toInt(arr(39)),
        Util2Type.toDouble(arr(40)),
        Util2Type.toDouble(arr(41)),
        Util2Type.toInt(arr(42)),
        arr(43),
        Util2Type.toDouble(arr(44)),
        Util2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        Util2Type.toInt(arr(57)),
        Util2Type.toDouble(arr(58)),
        Util2Type.toInt(arr(59)),
        Util2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        Util2Type.toInt(arr(73)),
        Util2Type.toDouble(arr(74)),
        Util2Type.toDouble(arr(75)),
        Util2Type.toDouble(arr(76)),
        Util2Type.toDouble(arr(77)),
        Util2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        Util2Type.toInt(arr(84))
      )
    })

    //构建DataFrame
    val df: DataFrame = sqlContext.createDataFrame(rowRDD,SchemaUtil.getSchemal)

    //存储为parquet文件
    df.write.parquet(outputpath)
    sc.stop()

  }

}
