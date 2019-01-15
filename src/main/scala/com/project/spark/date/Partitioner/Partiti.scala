package com.project.spark.date.Partitioner

import org.apache.spark.Partitioner

import scala.collection.mutable

class Partiti {

}

class Commodity(keys: Array[String]) extends Partitioner {
  //一共有10个商品 分10个区
  override def numPartitions: Int = keys.length

  val map = new mutable.HashMap[String, Int]
  for (i <- 0 until keys.length) {
    map.put(keys(i), i)
  }

  override def getPartition(key: Any): Int = {
    val str: String = key.asInstanceOf[String]
    map.getOrElse(str, 0)
  }
}