package com.project.spark.date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object text2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("date").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sss: RDD[(Int, Array[Int])] = sc.parallelize(ArrayBuffer((1, Array(1, 2, 3)), (2, Array(4, 5, 6)), (3, Array(1, 2, 3))))
    val ddd: RDD[(Int, Array[Int])] = sc.parallelize(ArrayBuffer((1, Array(3, 2, 1)), (2, Array(4, 5, 6)), (3, Array(9, 7, 8))))
    val unit: RDD[(Int, (Array[Int], Array[Int]))] = sss.join(ddd)
    val value: RDD[(Int, (Array[Int], Array[Int]))] = unit.filter(x => {
      x._2._1 != x._2._2
    })



  }
}
