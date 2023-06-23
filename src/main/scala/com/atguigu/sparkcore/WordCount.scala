package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val fileRDD = sc.textFile("data/*")
        val res = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
        res.foreach(println)
        sc.stop()
    }
}