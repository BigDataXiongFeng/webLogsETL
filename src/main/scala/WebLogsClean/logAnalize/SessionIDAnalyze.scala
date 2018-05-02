package WebLogsClean.logAnalize

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import WebLogsClean.utils.Util

/**
  * 将SessionID聚合
  * 统计一次会话中的各访问指标
  * 将结果保存到hive
  * xiongfeng
  * 2018/03/10
  */
object VisitsInfo {

  val sdf_origin = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)
  val sdf_standard = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
  val sdf_hdfsfolder = new SimpleDateFormat("yy-MM-dd")

  //自定义的将日志信息按服务器时间升序排序
  def dateComparator(elementA:String ,elementB:String):Boolean = {
    elementA.split(",")(12) < elementB.split(",")(12)
  }


  //自定义方法获取基于session分析的指标
  def getVisitsInfo(logInfoGroup:List[Row]):Array[String] = {

    //List中类型为Row时，使用以下语法：
    val deviceNum = logInfoGroup(0)(8).toString
    val customerId = logInfoGroup(0)(9).toString
    val sessionID = logInfoGroup(0)(10).toString
    val startTime = logInfoGroup(0)(12).toString
    val endTime = logInfoGroup(logInfoGroup.length-1)(12).toString
    val entryPage = logInfoGroup(0)(19).toString
    val leavePage = logInfoGroup(logInfoGroup.length-1)(19).toString
    val visitPageNum = logInfoGroup.map(log => (log(19),log)).groupBy(x => x._1).count(p => true).toString

    //List中类型为String时，使用以下语法：
    //    //获取该次会话的访客唯一标识
    //    val deviceNum = logInfoGroup(0).split("\t")(8)
    //
    //    //获取该次会话的会员Id
    //    val customerId = logInfoGroup(0).split("\t")(9)
    //
    //    //获取该次会话的sessionID
    //    val sessionID = logInfoGroup(0).split("\t")(10)
    //
    //    //获取该次会话的开始时间
    //    val startTime = logInfoGroup(0).split("\t")(12)
    //
    //    //获取该次会话的结束时间
    //    val endTime = logInfoGroup(logInfoGroup.length-1).split("\t")(12)
    //
    //    //获取该次会话第一次访问的url
    //    val entryPage = logInfoGroup(0).split("\t")(19)
    //
    //    //获取该次会话最后一次访问的url
    //    val leavePage = logInfoGroup(logInfoGroup.length-1).split("\t")(19)
    //
    //    //获取该次会话里所访问的页面总数
    //    //先用map函数将某次session里的所有访问记录变成(url,logInfo)元组的形式,然后再用groupBy函数按url分组,最后统计共有几个组
    //    val visitPageNum = logInfoGroup.map(log => (log.split("\t")(19),log)).groupBy(x => x._1).count(p => true)

    //访客唯一标识、会员ID、sessionID、会话起始时间、会话结束时间、进入页面url、离开页面url、访问页面数
    return Array(deviceNum,customerId,sessionID,startTime,endTime,entryPage,leavePage,visitPageNum)
  }


  //加载到hive分区表
  //此方法不同于Util中的saveToHiveTable，因为startTime字段不同
  def saveToHiveTable(spark: SparkSession,arr: Array[Int],yesterday: String,targetTableName: String,TempViewName: String): Unit = {

    var startHour = ""
    var endHour = ""

    for(hour <- arr){
      if(hour < 10){
        if(hour == 9){
          startHour = yesterday + "-0" + hour + ":00:00"
          endHour = yesterday + "0" + (hour + 1) + ":00:00"
          spark.sql("insert into table "+targetTableName+" partition(day='"+yesterday+"',hour='0"+hour+"') select * from "+TempViewName+" where startTime between '"+startHour+"' and '"+endHour+"'")
        }else{
          startHour = yesterday + "-0" + hour + ":00:00"
          endHour = yesterday + "-0" + (hour + 1) + ":00:00"
          spark.sql("insert into table "+targetTableName+" partition(day='"+yesterday+"',hour='0"+hour+"') select * from "+TempViewName+" where startTime between '"+startHour+"' and '"+endHour+"'")
        }
      }else{
        startHour = yesterday + "-" + hour + ":00:00"
        endHour = yesterday + "-" + (hour + 1) + ":00:00"
        spark.sql("insert into table "+targetTableName+" partition(day='"+yesterday+"',hour='"+hour+"') select * from "+TempViewName+" where startTime between '"+startHour+"' and '"+endHour+"'")
      }
    }
    println("save finish")
  }



  def main(args: Array[String]) {

//    val logFile = "hdfs://192.168.70.128:9000/spark_clickstream/session_log/"+VisitsInfo.sdf_hdfsfolder.format(curDate) // Should be some file on your system
//    val yesterday = Util.getYesterday()
    val yesterday = "2018-03-27"
    val sourceTableName = "ods_kunpu_test_db.ods_mami_source_test"
    val targetTableName = "ods_kunpu_test_db.dwd_user_session"
    val TempViewName = "SessionVisitTable"

    //构建spark上下文
    val spark  = SparkSession.builder()
      .appName("SeesionIDAnalyze")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    //获得partitions中的hour集合
    val timeDF = spark.sql("show partitions "+sourceTableName+" partition(day='"+yesterday+"')")

    val arr = Util.getHourList(timeDF)

    arr.foreach(println(_))

    //读取数据源
    //val sourceRDD = sc.textFile(logFile,1).cache()

    //读ods_mami_source_test表获得DataFrame
    val sourceDF = spark.sql("select * from "+sourceTableName+" where day='"+yesterday+"'")

    //DF转换成RDD
    val sourceRDD = sourceDF.rdd

    //将log信息变为(session,log信息)的tuple格式,也就是按session将log分组
    val logLinesPairMapRDD = sourceRDD.map(line => (line(10),line)).groupByKey()

    //对每个(session[String],log信息[Iterator<String>])中的日志按时间的升序排序
    //排完序后(session[String],log信息[Iterator<String>])的格式变为log信息[Iterator<String>]
    val sortedLogRDD = logLinesPairMapRDD.map(_._2.toList.sortWith((A,B) => dateComparator(A.toString(),B.toString())))

    //统计每一个单独的Session的相关信息
    val resultRDD = sortedLogRDD.map(getVisitsInfo(_))
    resultRDD.cache()

    //关联case class
    val SessionClassRDD = resultRDD.map(x => SessionInfoClass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
    SessionClassRDD.foreach(println(_))

    //创建临时视图
    import spark.sqlContext.implicits._
    SessionClassRDD.toDF().createOrReplaceTempView(TempViewName)

    //加载到hive中间表
    saveToHiveTable(spark, arr, yesterday, targetTableName, TempViewName)
    spark.stop()
  }
}

case class SessionInfoClass(deviceNum: String,customerId: String,sessionID: String,startTime: String,endTime: String,entryPage: String,leavePage: String,visitPageNum: String)


