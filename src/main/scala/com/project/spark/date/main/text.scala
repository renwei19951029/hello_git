package com.project.spark.date.main

import java.util.Date

object text {
  def main(args: Array[String]): Unit = {
    val t = new Date()
    println(t.toString)
    val strings: Array[String] = t.toString.split(":")(0).split(" ")
    println(strings.last)
  }
}
