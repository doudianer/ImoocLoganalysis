package com.wl.Log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/*
*
* */
object StatDAO {
  /*
  * 统计imooc主站最受欢迎课程/手记的topn访问次数
  * */
  def insertDayVideotopn(list:ListBuffer[DayVideoAccessStat]):Unit={
    var connect:Connection=null
    var pstmt:PreparedStatement=null
    try{
      connect=MySQLUtils.getConnection()
      connect.setAutoCommit(false)//设置手动提交
      val sql="insert into day_video_access_topn_stat(day,cmsId,times) values(?,?,?)"
      pstmt=connect.prepareStatement(sql)
      for (ele<-list)
        {
          pstmt.setString(1,ele.day)
          pstmt.setLong(2,ele.cmsId)
          pstmt.setLong(3,ele.time)
          pstmt.addBatch()
        }
      pstmt.executeBatch()
      connect.commit()
    }

  }

  /*
  * 按地市统计imooc主站最受欢迎topn课程
  * */
  def insertDayCityVideotopn(list:ListBuffer[DayCityVideoAccessStat]):Unit={
    var connect:Connection=null
    var pstmt:PreparedStatement=null
    try{
      connect=MySQLUtils.getConnection()
      connect.setAutoCommit(false)//设置手动提交
      val sql="insert into day_video_city_access_topn_stat (day,cmsId,city,times,times_rank) values(?,?,?,?,?)"
      pstmt=connect.prepareStatement(sql)
      for (ele<-list)
      {
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setString(3,ele.city)
        pstmt.setLong(4,ele.times)
        pstmt.setInt(5,ele.times_rank)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connect.commit()
    }

  }
  /*
  * 按流量统计imooc主站最受欢迎的topn课程
  * */
  def insertDayVideoTrafficstopn(list:ListBuffer[DayVideoTrafficsStat]):Unit={
    var connect:Connection=null
    var pstmt:PreparedStatement=null
    try{
      connect=MySQLUtils.getConnection()
      connect.setAutoCommit(false)//设置手动提交
      val sql="insert into  day_video_traffics_topn_stat(day,cmsId,traffics) values(?,?,?)"
      pstmt=connect.prepareStatement(sql)
      for (ele<-list)
      {
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.traffics)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connect.commit()
    }

  }
  def deldata(day:String): Unit =
  {
    val tables=Array("day_video_access_topn_stat","day_video_city_access_topn_stat","day_video_traffics_topn_stat")
    var connect:Connection=null
    var psmt:PreparedStatement=null
    try{
      connect=MySQLUtils.getConnection()
      for(table<-tables)
        {
          val delSQL=s"delete from $table where day=?"
          psmt=connect.prepareStatement(delSQL)
          psmt.setString(1,day)
          psmt.executeUpdate()
        }

    }catch
      {
        case e:Exception=>e.printStackTrace()
      }
    finally {
      MySQLUtils.release(connect,psmt)
    }
  }



}
