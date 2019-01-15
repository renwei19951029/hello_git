package com.project.spark.date.IPUtil

import scala.collection.mutable.ArrayBuffer

object IPUtil {
  //传入一个ip规则库 和一个date的数组   返回一个新的数组带有地址列的
  def addDiZhilie(rdd: Array[Array[String]], ipGZ: Array[(Long, Long, String)]): ArrayBuffer[Array[String]] = {
    val newArrays: ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]
    //for循环传进来的数据
    for (i <- 0 until rdd.length) {
      //获取每一行数据
      val strings: Array[String] = rdd(i)
      //获取到ip
      val ip: String = strings(0)
      //将ip转换成Long类型的数据
      val ipLong: Long = ipStringToLong(ip)
      //用二分法查找地址
      val diZhiIndex: Int = ipGZerfenfa(ipLong, ipGZ)
      //ip所属省
      val sheng: String = ipGZ(diZhiIndex)._3
      //得到了新的带有省的列
      val newArray = Array[String](strings(0), strings(1), strings(2), strings(3), sheng)
      //将新的列放入了一个中
      newArrays.+=(newArray)
    }
    newArrays
  }

  //传入一个String ip   返回Long类型
  def ipStringToLong(ip: String): Long = {
    val ips = ip.split("[.]")
    var ipNum = 0L
    //ip的特定算法   可以将ip转换成数字
    for (i <- 0 until ips.length) {
      ipNum = ips(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //用ip在ip规则库查找省
  def ipGZerfenfa(ip: Long, ipGZ: Array[(Long, Long, String)]): Int = {
    var c = 0
    var j = ipGZ.length
    while (c <= j) {
      //中间值
      var z = (c + j) / 2
      //判断查询的ip是否在当前中间值所在的Array的范围中
      if (ip >= ipGZ(z)._1 && ip <= ipGZ(z)._2) {
        return z
      } else if (ip < ipGZ(z)._1) {
        j = z - 1
      } else {
        c = z + 1
      }
    }
    -1
  }
}
