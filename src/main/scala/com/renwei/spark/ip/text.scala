package com.renwei.spark.ip

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object text extends App {

  val conf: SparkConf = new SparkConf().setAppName("IPCount").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val ips: RDD[String] = sc.textFile("D:\\ip.txt")
  //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
  val ipGZ: RDD[(Long, Long, String)] = ips.map(_.split("[|]")).map(x => (x(2).toLong, x(3).toLong, x(6)))
  val ipGZs = ipGZ.collect()
  private val i: Int = IPUtil.IP2Seek("1.2.0.0", ipGZs)
  println(i)
}
