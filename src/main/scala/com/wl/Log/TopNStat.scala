package com.wl.Log

import org.apache.spark.sql.{SaveMode, SparkSession}

object TopNStat {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SparkStatFormatJob")
        //.config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      //.master("local[2]")
      .getOrCreate()
    val access=spark.sparkContext.textFile("file:///home/hadoop/data/access.log")

    //RDD转化为DF
    val accessDF=spark.createDataFrame(access.map(x=>AccessConvertUtil.parse(x)),AccessConvertUtil.struct)
    //输出schema
    accessDF.printSchema()
    accessDF.show(10)
    accessDF.write.format("parquet").save("/imooclog")
    spark.stop()
  }

}
