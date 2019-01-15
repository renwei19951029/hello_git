package com.renwei.spark.ip

object IPUtil {
  def IP2Seek(ip: String, ipGZ: Array[(Long, Long, String)]): Int = {
    val ips = ip.split("[.]")
    var ipNum = 0L
    //ip的特定算法   可以将ip转换成数字
    for (i <- 0 until ips.length) {
      ipNum = ips(i).toLong | ipNum << 8L
    }
    //二分法在ip的规则的Array中查找

    f2(ipNum, ipGZ)
  }

  def f2(ip: Long, ipGZ: Array[(Long, Long, String)]): Int = {
    var c = 0
    var index = -1
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
