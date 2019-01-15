package com.project.spark.date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object text {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("date").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val d: RDD[String] = sc.textFile("D:\\shuju2.txt")
    val d01: RDD[(String, String, String)] = d.flatMap(_.split("\n")).map(_.split("\t")).map(x => {
      (x(1), x(2), x(4))
    })
    val d03: RDD[((String, String), String)] = d01.map(x => {
      ((x._3, x._1), x._2)
    })
    val d04: RDD[((String, String), Iterable[String])] = d03.groupByKey()
    val d05: RDD[(String, (String, Int, Int))] = d04.map(x => {
      (x._1._1, (x._1._2, x._2.toList.filter(_ == "购买").length, x._2.toList.length - x._2.toList.filter(_ == "购买").length))
    })
    val d06: RDD[(String, Iterable[(String, Int, Int)])] = d05.groupByKey()
    val d07: RDD[(String, (String, Int, Int))] = d06.map(x => {
      val tuple: (String, Int, Int) = x._2.toList.sortBy(y => {
        (y._2, y._3)
      }).reverse(0)
      (x._1, tuple)
    })

    val d08: Array[(String, String)] = d07.map(x => {
      (x._1, x._2._1)
    }).collect()

    //找出每个用户都买过什么商品
    //d10就是所有的用户都买过什么东西   带有评分
    val d10: RDD[(String, List[(String, Int, Int)])] = d06.map(x => {
      val ssssss: List[(String, Int, Int)] = x._2.toList.filter(_._2 > 0).sortBy(y => {
        (y._2, y._3)
      }).reverse
      (x._1, ssssss)
    })
    //下面求出都有谁买过d08数组中的东西 ArrayBuffer((88,List((天翼,3,4), (三星,3,1), (科威,2,3), (oppo,1,5), (苹果,1,4), (联想,1,3))), (4,List((小米,2,4), (oppo,2,3), (锤子,1,5), (天翼,1,4))))
    val map = new mutable.HashMap[String, RDD[(String, List[(String, Int, Int)])]]

    //ArrayBuffer((88,天翼), (4,小米))   d08
    //循环遍历 d08 取出应该推荐的商品 在d10每个人买过的所有商品过滤
    for (i <- 0 until d08.length) {
      val sp: String = d08(i)._2
      val ddd: RDD[(String, List[(String, Int, Int)])] = d10.map(x => {
        val tuples: List[(String, Int, Int)] = x._2.toList.filter(_._1 == sp)
        (x._1, tuples)
      })
      val tuple: (String, String) = d08(i)
      val d11: RDD[(String, List[(String, Int, Int)])] = ddd.filter(x => {
        x._2.length > 0
      })
      map.put(tuple._1 + "^A" + tuple._2, d11)
    }
    //去重   去掉被推荐人本身买过这个商品的记录
    val d12: mutable.HashMap[String, Array[(String, List[(String, Int, Int)])]] = map.map(x => {
      val tuples: Array[(String, List[(String, Int, Int)])] = x._2.collect().filter(y => {
        y._1 != x._1
      })
      (x._1, tuples)
    })
    //只要两条记录
    val d13: mutable.HashMap[String, Array[String]] = d12.map(x => {
      val strings: Array[String] = x._2.map(y => {
        y._1
      })
      (x._1, strings)
    })
    //都有谁买过这个商品   71^A苹果   ArrayBuffer(88, 82, 80, 62, 86, 28, 59, 66, 84, 39, 64, 8, 57, 20, 55, 4）
    val d14: Array[(String, Array[String])] = d13.toArray
    val map02: mutable.HashMap[String, Array[Array[String]]] = new mutable.HashMap[String, Array[Array[String]]]
    //71^A苹果 88  ArrayBuffer(CompactBuffer((科威,2,3), (联想,1,3), (神州,0,2), (三星,3,1), (苹果,1,4), (oppo,1,5), (小米,0,2), (戴尔,0,4), (锤子,0,4), (天翼,3,4)))
    //所有人买过什么商品，和d14做右外链接，得到每个商品的评分
    val d16: Array[(String, RDD[(String, Option[Iterable[(String, Int, Int)]])])] = d14.map(x => {
      //x.2是数组
      val value: RDD[(String, Option[Iterable[(String, Int, Int)]])] = sc.parallelize(x._2.map((_, 1))).leftOuterJoin(d06).map(x => {
        (x._1, x._2._2)
      })
      (x._1, value)
    })



    /* val d17: Array[(String, Array[(String, Int, Int)])] = d16.map(x => {
       val flatten: Array[(String, Int, Int)] = x._2.collect().map(y => {
         val flatten: List[(String, Int, Int)] = y._2.map(z => {
           val tuples: List[(String, Int, Int)] = z.toList.filter(c => {
             //将最里层的0的去掉
             c._2 != 0
           })
           tuples
         }).toList.flatten
         flatten
       }).flatten
       (x._1, flatten)
     })

     //这个是所有被推荐人
     val d19: Array[String] = d17.map(x => {
       x._1.split("\\^A")(0)
     })

     //现在就是将所有被推荐人买过的东西 和推荐人买多的东西取差集
     val d20: Map[String, Iterable[(String, Int, Int)]] = d06.collect().toMap
     val d21: Array[(String, List[(String, Int, Int)])] = d19.map(x => {
       (x, d20.get(x).toList.flatten.filter(z => {
         z._2 != 0
       }))
     })
     val d017: Array[(String, Array[(String, Int, Int)])] = d17.map(x => {
       (x._1.split("\\^A")(0), x._2)
     })

     //用这个和哪个取差集
     val d0017: Array[(String, Array[String])] = d017.map(x => {
       val strings: Array[String] = x._2.sortBy(y => (y._2, y._3)).map(z => {
         z._1
       })
       (x._1, strings)
     })

     val d021: Array[(String, Array[String])] = d21.map(x => {
       val strings: List[String] = x._2.map(y => {
         y._1
       })
       (x._1, strings.toArray)
     })

     val d23: RDD[(String, (Array[String], Array[String]))] = sc.parallelize(d0017).join(sc.parallelize(d021))
     val value: RDD[(String, Set[String])] = d23.map(x => {
       (x._1, x._2._1.toSet -- x._2._2.toSet)
     })
     val values: RDD[(String, String)] = d.map(x => {
       (x._1, x._2.head)
     })

 */
    sc.stop()
  }

}
