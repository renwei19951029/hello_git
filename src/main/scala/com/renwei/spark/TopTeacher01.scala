package com.renwei.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopTeacher01 extends App {
  val conf = new SparkConf().setAppName("work01").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val date = sc.textFile("D:\\软件\\飞秋\\feiq\\Recv Files\\teacher.log")
  //现将data中的老师名字和学科取出来
  //http://bigdata.bw.cn/laoli
  val dateLine: RDD[((String, String), Int)] = date.map(line => {
    //每一行数据
    val strings = line.split("/")
    //老师的名字
    val teacherName = strings(3)
    //学科
    val subject = strings(2).split("\\.")(0)
    ((teacherName, subject), 1)
  })

  //将相同的加起来
  private val value: RDD[((String, String), Int)] = dateLine.reduceByKey(_ + _)
  //再按照学科分组
  private val groups: RDD[(String, Iterable[((String, String), Int)])] = value.groupBy(_._1._2)
  //学科组内排序
  private val sort: RDD[(String, List[((String, String), Int)])] = groups.mapValues(it => {
    it.toList.sortBy(_._2).reverse
  })
  println(sort.collect().toBuffer)
}
