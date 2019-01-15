package com.renwei.spark.ip

object text02 extends App {
  var a = 0
  var b = 100
  val d = 30
  while (a < b) {
    var c = (a + b) / 2
    if (d == c) {
      println("正好")
      println(a + "   " + b + "   " + c + "    " + d)
      a = b
    } else if (d < c) {
      println("小")
      println(a + "   " + b + "   " + c + "    " + d)
      b = c - 1
    } else {
      println("大")
      println(a + "   " + b + "   " + c + "    " + d)
      a = c + 1
    }
    Thread.sleep(2000)
  }
}
