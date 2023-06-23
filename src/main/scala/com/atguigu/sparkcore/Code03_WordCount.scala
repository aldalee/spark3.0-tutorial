package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object Code03_WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("data/*")
        val res = lines
          .flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
          .collect()
        res.foreach(println)
        sc.stop()
    }
}
