package com.luodesong.tag.thetrait

import scala.collection.mutable._
/**
  * 标签的统一接口
  */
trait Tag {
    /**
      * 统一标签的方法
      */
    def makeTags(args:Any*):ListBuffer[(String, Int)]
}
