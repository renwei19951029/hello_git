package com.project.spark.date

import java.io.{File, PrintWriter}
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object getDate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("date").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val value = sc.textFile("D:\\20090121000132.394251.http.format")
    val ipDates: RDD[String] = value.map(_.split("[|]")).map(_ (1))
    val ipArrays: Array[String] = ipDates.collect()
    val buffer: StringBuffer = new StringBuffer()
    val spArrays: Array[String] = Array("戴尔", "联想", "科威", "神州", "苹果", "小米", "天翼", "三星", "锤子", "oppo")
    val typeArrats = Array("浏览", "浏览", "浏览", "浏览", "浏览", "购买", "购买")
    val random: Random = new Random()
    val inclusives: Array[Range.Inclusive] = Array(1 to (100))

    for (u <- 0 until ipArrays.length) {
      val date: Date = new Date(System.currentTimeMillis())
      val i = random.nextInt(86400).toLong * 1000L
      val time = date.getTime - 32400L * 1000L - i
      val s: Int = inclusives(0)(random.nextInt(100))
      val a = ipArrays(u) + "\t" + spArrays(random.nextInt(10)) + "\t" + typeArrats(random.nextInt(6)) + "\t" + time + "\t" + s + "\n"
      buffer.append(a)
    }
    val writer = new PrintWriter(new File("D:\\shuju2.txt"))
    writer.write(buffer.toString)
    writer.close()
  }

}
