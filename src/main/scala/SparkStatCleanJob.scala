package com.wl.Log

import org.apache.spark.sql.SparkSession
/*
*
* 使用Spark完成数据清洗
* */

object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    val access=spark.sparkContext.textFile("")
    //RDD转化为DF
    val accessDF=spark.createDataFrame(access.map(x=>AccessConvertUtil.parse(x)),AccessConvertUtil.struct)
    //输出schema
    accessDF.printSchema()
    accessDF.show(20)

    spark.stop()
  }


}
