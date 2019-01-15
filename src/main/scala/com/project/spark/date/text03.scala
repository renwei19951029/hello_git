package com.project.spark.date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object text03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("work").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val date01: RDD[String] = sc.textFile("D:\\软件\\飞秋\\feiq\\Recv Files\\上机\\HTTP_20130313143750.dat")
    val d02: RDD[Array[String]] = date01.flatMap(_.split("\n")).map(_.split("\t"))
    val d03: RDD[(String, (Int, Int, Int))] = d02.map(x => {
      (x(1), (x(8).toInt + x(9).toInt, x(8).toInt, x(9).toInt))
    })
    val d04: RDD[(String, (Int, Int, Int))] = d03.reduceByKey((x, y) => {
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    })
    val d05: RDD[(String, (Int, Int, Int))] = d04.sortBy(x => {
      (x._2._1, x._2._2, x._2._3)
    }, false)

    println(d05.collect()(0).toString())
    println(d05.collect()(1).toString())
    println(d05.collect()(2).toString())

    sc.stop()

  }
}
