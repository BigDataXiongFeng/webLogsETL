package WebLogsClean.utils


import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  *将公共使用的方法封装到Util类中
  */
object Util {

  //定义方法，获得hour的集合
  def getHourList(timeDF: DataFrame): Array[Int] = {
    val hourRDD = timeDF.rdd.map(x => {
      val list = x.toString().split("]")(0).split("=")
      val hour = list(2)
      hour.toInt
    })
    val arr = hourRDD.collect()
    arr
  }


  //获得昨天的日期
  def getYesterday(): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    yesterday
  }


  //加载到hive分区表
  def saveToHiveTable(spark: SparkSession,arr: Array[Int],yesterday: String,targetTableName: String,TempViewName: String): Unit = {
    var startHour = ""
    var endHour = ""
    for(hour <- arr){
      if(hour < 10){
        startHour = yesterday + "-0" + hour + ":00:00"
        endHour = yesterday + "-0" + (hour + 1) + ":00:00"
        spark.sql("insert into table "+targetTableName+" partition(day='"+yesterday+"',hour='0"+hour+"') select * from "+TempViewName+" where serverTime between '"+startHour+"' and '"+endHour+"'")
      }else{
        startHour = yesterday + "-" + hour + ":00:00"
        endHour = yesterday + "-" + (hour + 1) + ":00:00"
        spark.sql("insert into table "+targetTableName+" partition(day='"+yesterday+"',hour='"+hour+"') select * from "+TempViewName+" where serverTime between '"+startHour+"' and '"+endHour+"'")
      }
    }
    println("save finish")
  }


//  定义函数，将数据导入到mysql中
    val data2MYSQL = (iterator: Iterator[(String, Int)]) => {
      var conn : Connection = null
      var ps : PreparedStatement = null
      //提前准备好一个表结构，包含三个字段
      val sql = "insert into location_info (province, counts, date)"

      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })

      if(ps != null)
        ps.close()
      if(conn != null)
        conn.close()
    }


  val day = new java.sql.Date(System.currentTimeMillis())
  val url = "jdbc:mysql://10.16.3.5:3306/"
  val user = "xiongfeng"
  val password = "MkoT32@lz"
  var conn : Connection = null
  var ps : PreparedStatement = null

  val sql2 = "insert into ods_db.dw_visitor_act_daliy_act  (uv_act, uv_new, day) values (?, ?, ?)"

}
