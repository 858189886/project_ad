package com.utils

/**
  * 数据类型的转换
  */
object Util2Type {
  //string转换为int
  def toInt(str:String):Int = {
    try{
      str.toInt
    }catch{
      case _:Exception => 0
    }
  }

  //string转换为double
  def toDouble(str:String):Double = {
    try{
      str.toDouble
    }catch{
      case _:Exception => 0.0
    }
  }
}
