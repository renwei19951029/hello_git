package com.renwei.spark.ip

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

object DBUtil {
  def setDB(it: Iterator[(String, Int)]) = {
    val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/renwei", "root", "renwei")
    val pre: PreparedStatement = connection.prepareStatement("insert into ip (chengshi,shuliang,time) values (?,?,?)")
    it.foreach(x => {
      pre.setString(1, x._1)
      pre.setInt(2, x._2)
      pre.setDate(3, new Date(System.currentTimeMillis()))
      pre.executeUpdate()
    })
    connection.close()
    pre.close()
  }
}
