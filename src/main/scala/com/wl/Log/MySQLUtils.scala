package com.wl.Log

import java.sql.{Connection,DriverManager, PreparedStatement}


/*
* 连接数据库
* */
object MySQLUtils {
  /*
  * 连接数据库
  * */
  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://hadoop:3306/imooc_project?useUnicode=true&characterEncoding=UTF-8&user=root&password=123456")
  }
  /*
  * 释放资源
  * */
  def release(connection:Connection,pstmt:PreparedStatement):Unit={
    try{
      if(pstmt!=null)
        {
          pstmt.close()
        }

    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      if(connection!=null)
        {
          connection.close()
        }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
