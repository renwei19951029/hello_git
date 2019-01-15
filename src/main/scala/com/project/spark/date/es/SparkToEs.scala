package com.project.spark.date.es

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object SparkToEs {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DecisionTree1").setMaster("local")
    sparkConf.set("cluster.name", "es")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "esrestful.motouat.com.cn")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.read.missing.as.empty","true")
    sparkConf.set("es.nodes.wan.only","true")
    val sc = new SparkContext(sparkConf)
    //write2Es(sc) //写
    read4Es(sc);   //读

  }

  def write2Es(sc: SparkContext) = {
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
    var rdd = sc.makeRDD(Seq(numbers, airports))
    EsSpark.saveToEs(rdd, "book/no")
    println("--------------------End-----------------")
  }

  def read4Es(sc: SparkContext) {
    val rdd = EsSpark.esRDD(sc, "new_retail_20181214/_doc/oLHCTjq7NKJDXT7bkv24PR2uVUI0/","?q=this*")

    println("------------------rdd.count():" + rdd.count())
    rdd.foreach(line => {
      val key = line._1
      val value = line._2
      println("------------------key:" + key)
      for (tmp <- value) {
        val key1 = tmp._1
        val value1 = tmp._2
        println("------------------key1:" + key1)
        println("------------------value1:" + value1)
      }
    })
  }

}