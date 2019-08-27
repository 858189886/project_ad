package com.utils

/**
  * 指标方法
  */
object LocationUtil {

  // 处理请求数
  def doRequest(reqMode:Int,prcMode:Int):List[Double]={
    if(reqMode ==1 && prcMode == 1){
      List[Double](1,0,0)
    }else if(reqMode ==1 && prcMode == 2){
      List[Double](1,1,0)
    }else if(reqMode == 1 && prcMode == 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  // 处理点击数
  def doCli(reqMode:Int,isffective:Int):List[Double]={
    if(reqMode == 2 && isffective ==1){
      List[Double](1,0)
    }else if(reqMode == 3 && isffective ==1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }

  // 处理竞价和广告成本消费
  def doAdv(isffective:Int,bill:Int,bid:Int,
            win:Int,ad:Int,winPrice:Double,adPayment:Double):List[Double]={
    if(isffective ==1 && bill ==1 && bid ==1){
      List[Double](1,0,0,0)
    }else if(isffective ==1 && bill ==1 && win == 1){
      if(ad != 0){
        List[Double](0,1,winPrice/1000,adPayment/1000)
      }else{
        List[Double](0,0,winPrice/1000,adPayment/1000)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
}