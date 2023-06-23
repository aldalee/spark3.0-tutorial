package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object Code02_WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("data/*")
        val res = lines
          .flatMap(_.split(" "))
          .map(word => (word, 1))
          .groupBy(t => t._1)
          .map {
              case (_, list) => {
                  list.reduce {
                      (t1, t2) => (t1._1, t1._2 + t2._2)
                  }
              }
          }
        res.foreach(println)
        sc.stop()
    }
}