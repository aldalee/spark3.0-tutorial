package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}


object Code01_WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("data/*")
        val res = lines
          .flatMap(_.split(" "))
          .groupBy(word => word)
          .map {
              case (word, list) => (word, list.size)
          }
          .collect()

        res.foreach(println)
        sc.stop()
    }
}
