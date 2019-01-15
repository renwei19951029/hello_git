package com.renwei.spark

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object TopTeacher02 extends App {
  //手动分区
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
  val arrays: Array[String] = dateLine.map(x => x._1._2).collect().distinct

  private val partitioner: SubjectPartitioner = new SubjectPartitioner(arrays)
  val values: RDD[((String, String), Int)] = dateLine.reduceByKey(partitioner, _ + _)
  println(values.collect().toBuffer)
  sc.stop()
}

//自定义分区类   继承Partitioner   传递一个参数   参数是Array   参数的length就是分区的数量
class SubjectPartitioner(var no: Array[String]) extends Partitioner {
  //这个是这个程序将有几个分区
  override def numPartitions: Int = no.length

  val map: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()
  var i = 0
  for (nos <- no) {
    map.put(nos, i)
    i = i + 1
  }

  //这个是分区的依据
  override def getPartition(key: Any): Int = {
    val k: (String, String) = key.asInstanceOf[(String, String)]
    map.getOrElse(k._2, 0)
  }
}
