package com.project.spark.date.main

import com.project.spark.date.IPUtil.IPUtil
import com.project.spark.date.Partitioner.Commodity
import com.project.spark.date.calculate.Calculate
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object MainClass {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("project").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    //获取原始数据
    val dateRDD: RDD[String] = sc.textFile(args(0))
    //将数据拆分
    val dateRDDMR: RDD[Array[String]] = dateRDD.flatMap(_.split("\n")).map(_.split("\t"))
    //将拆分好的数据放在缓存中
    val dateRDDCache: RDD[Array[String]] = dateRDDMR.cache()
    //获取IP规则库
    val ips: RDD[String] = sc.textFile(args(1))
    //修改ip规则库 取出有用的列
    val ipGZ: RDD[(Long, Long, String)] = ips.map(_.split("[|]")).map(x => (x(2).toLong, x(3).toLong, x(6)))
    //将ip规则聚合起来
    val ipGZs = ipGZ.collect()
    //将ip规则库变成广播变量   能让全部的work都能用到
    val bc: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipGZs)
    //将数据按ip地址多生成一行地址的列
    val newDateArrayAddDiZhi: ArrayBuffer[Array[String]] = IPUtil.addDiZhilie(dateRDDCache.collect(), bc.value)
    //将带有地址的Array变成RDD
    val dateAndDZRDD: RDD[Array[String]] = sc.parallelize(newDateArrayAddDiZhi)
    //再将这个rdd缓存起来
    val dateAndDZRDDcache: RDD[Array[String]] = dateAndDZRDD.cache()

    //需求1，各商品分区，统计各商品在各地区的销量 点击量，先按销量排序，在按点击量排序
    //ArrayBuffer((联想,陕西,33,145), (天翼,云南,4,9))需求01的返回值
    val xuqiu01RDD: RDD[(String, String, Int, Int)] = Calculate.xuqiu1(dateAndDZRDDcache)
    //将需求01的结果缓存起来 留着需求02用
    val jGRDDCache: RDD[(String, String, Int, Int)] = xuqiu01RDD.cache()
    //需求3，按商品分区，各时间段的点击量，销量情况，按点击量，销量排序
    //ArrayBuffer((联想,小时,33,145), (天翼,小时,4,9))需求03的返回值
    val xuqiu03RDD: RDD[(String, String, Int, Int)] = Calculate.xuqiu3(dateAndDZRDDcache)
    //将缓存中的数据变成需求01所需要的格式
    val spAndJg01: RDD[(String, (String, Int, Int))] = jGRDDCache.map(x => {
      (x._1, (x._2, x._3, x._4))
    })
    //将缓存中的数据变成需求02所需要的格式
    val dqAndJg02: RDD[(String, (String, Int, Int))] = jGRDDCache.map(x => {
      (x._2, (x._1, x._3, x._4))
    })
    //将需求03的结果变成所需要的格式
    val spAndJg03: RDD[(String, (String, Int, Int))] = xuqiu03RDD.map(x => {
      (x._1, (x._2, x._3, x._4))
    })
    //为以商品分区做的准备
    val spkeys: Array[String] = spAndJg01.keys.collect().distinct
    //为以地区分区做的准备
    val dikeys: Array[String] = dqAndJg02.keys.collect().distinct
    //将需求01的结果按照商品分区
    val spJg01: RDD[(String, (String, Int, Int))] = spAndJg01.partitionBy(new Commodity(spkeys))
    //将需求02的结果按照地区分区
    val diJg02: RDD[(String, (String, Int, Int))] = dqAndJg02.partitionBy(new Commodity(dikeys))
    //将需求03的结果按照商品分区
    val spJg03: RDD[(String, (String, Int, Int))] = spAndJg03.partitionBy(new Commodity(spkeys))
    //需求01组内排序
    val jg01: RDD[(String, (String, Int, Int))] = spJg01.mapPartitions(y => {
      y.toList.sortBy(x => (x._2._2, x._2._3)).reverse.iterator
    })
    //需求02组内排序
    val jg02: RDD[(String, (String, Int, Int))] = diJg02.mapPartitions(y => {
      y.toList.sortBy(x => (x._2._2, x._2._3)).reverse.iterator
    })
    //需求01组内排序
    val jg03: RDD[(String, (String, Int, Int))] = spJg03.mapPartitions(y => {
      y.toList.sortBy(x => (x._2._2, x._2._3)).reverse.iterator
    })
    val d01: RDD[(String, String)] = Calculate.tuijian(dateRDD, sc)
    //需求01结果写入hdfs
    jg01.saveAsTextFile(args(2))
    //需求02结果写入hdfs
    jg02.saveAsTextFile(args(3))
    //需求03结果写入hdfs
    jg03.saveAsTextFile(args(4))
    d01.saveAsTextFile(args(5))
    //关闭资源
    sc.stop()
  }
}
