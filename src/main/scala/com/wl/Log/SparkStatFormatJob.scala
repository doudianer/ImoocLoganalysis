package com.wl.Log

import org.apache.spark.sql.SparkSession

/*
* 第一步清洗，抽取指定列数据
* */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access=spark.sparkContext.textFile("")




    spark.stop()
  }


}
