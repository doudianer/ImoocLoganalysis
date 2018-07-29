package com.wl.Log


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object TopNStatJob {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("TopNStatJob")
      //.config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]")
      .getOrCreate()
    val access=spark.sparkContext.textFile("C:\\Users\\wl105\\Desktop\\imooclog\\access.log")

    //RDD转化为DF
    val accessDF=spark.createDataFrame(access.map(x=>AccessConvertUtil.parse(x)),AccessConvertUtil.struct).show(20)

    val day="20170511"
    StatDAO.deldata(day)
    //最受欢迎课程
   // videoAccessTopNStat(spark,accessDF,day)
    //按照地市分组最受欢迎课程
    //cityAccessTopNStat(spark,accessDF,day)
    //按照流量统计最受欢迎的课程
    //videoTrafficAccessTopNStat(spark,accessDF,day)
  }

  /*
  * 罪受欢迎的Topn课程
  * */

  def videoAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String)={

    import spark.implicits._
   // val videotopn=accessDF.filter($"day"==="20170511"&&$"cmsType"==="video")
     // .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times")
    //videotopn.show(false)
     accessDF.createOrReplaceTempView("access_log")
    val videotopn=spark.sql(s"select day,cmsId,count(1) as times from access_log where day=$day and cmsType='video' group by  day,cmsId order by times desc")
    videotopn.show(false)
    //将统计结果写数据库
   try {
      videotopn.foreachPartition(p => {
        var list = new ListBuffer[DayVideoAccessStat]
        p.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDAO.insertDayVideotopn(list)
      })
    }catch {
      case e:Exception=>e.printStackTrace()
    }

  }
  /*
  * 按照地市统计topn课程
  * */

    def cityAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String): Unit =
  {

    accessDF.createOrReplaceTempView("access_log")

    import spark.implicits._
    val citytopn=accessDF.filter($"day"===day)
      .groupBy("day","city","cmsId").agg(count("cmsId").as("times")).orderBy($"times")
    citytopn.show(false)
    val top3DF=citytopn.select(citytopn("day"),
      citytopn("city"),
      citytopn("cmsId"),
      citytopn("times"),
      row_number().over(Window.partitionBy(citytopn("city")).orderBy(citytopn("times").desc)).as("times_rank")
    ).filter("times_rank<=3")

  try {
      top3DF.foreachPartition(p => {
        var list = new ListBuffer[DayCityVideoAccessStat]
        p.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city=info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val times_rank=info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day,cmsId,city,times,times_rank))
        })
        StatDAO.insertDayCityVideotopn(list)
      })
    }catch {
      case e:Exception=>e.printStackTrace()
    }
  }
  def videoTrafficAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String): Unit =
  {
    accessDF.createOrReplaceTempView("access_log")
    import spark.implicits._
    val traffics=accessDF.filter($"day"===day&&$"cmsType"==="video")
      .groupBy("day","cmsId").agg(sum("traffic")as("traffics")).orderBy($"traffics".desc)//.show(false)
    try {
      traffics.foreachPartition(p => {
        val list = new ListBuffer[DayVideoTrafficsStat]
        p.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })
        StatDAO.insertDayVideoTrafficstopn(list)
      })
    }catch {
      case e:Exception=>e.printStackTrace()
    }
  }



}
