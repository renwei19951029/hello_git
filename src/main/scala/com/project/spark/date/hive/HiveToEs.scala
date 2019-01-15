package com.project.spark.date.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveToEs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HiveToEs").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val context = new HiveContext(sc)
    val date = context.sql("select * from myhive.lx_user")
    date.show()
  }
}
