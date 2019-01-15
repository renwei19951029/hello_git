package com.renwei.spark.ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IPCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IPCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val dateRDD: RDD[String] = sc.textFile(args(0))
    //将数据切分   切出ip地址
    val ipArrays: RDD[String] = dateRDD.map(_.split("[|]")).map(_ (1))
    //引入ip规则库
    val ips: RDD[String] = sc.textFile(args(1))
    //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    val ipGZ: RDD[(Long, Long, String)] = ips.map(_.split("[|]")).map(x => (x(2).toLong, x(3).toLong, x(6)))
    val ipGZs = ipGZ.collect()
    val bc: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipGZs)
    val ipDiZhis: RDD[(String, Int)] = ipArrays.map(x => {
      val sheng: Int = IPUtil.IP2Seek(x, bc.value)
      (ipGZs(sheng)._3, 1)
    })
    val DiZhiS: RDD[(String, Int)] = ipDiZhis.reduceByKey(_ + _)
    DiZhiS.foreachPartition(x => {
      DBUtil.setDB(x)
    })

  }
}
