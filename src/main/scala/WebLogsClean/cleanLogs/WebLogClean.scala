package WebLogsClean.cleanLogs

import java.util.{Calendar}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Locale


/**
  * 生产集群代码
  * 对日志进行清洗，过滤掉无效或缺失关键字段的日志信息
  * 过滤后的日志，一份保留到hdfs，一份存到hive原始表中
  * 2018/02/27
  * xiongfeng
  */
class WebLogClean extends Serializable{

  val sdf_origin = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)
  val sdf_standard = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
  val sdf_hdfsfolder = new SimpleDateFormat("yyyy-MM-dd")


  //日志过滤逻辑
  def weblogParser(logLine: String): String = {

    //过滤掉信息不全或者格式不正确的日志信息,按照事件字段个数(true or false)
    val fields = logLine.split("\t")
    val isStandardLogInfo = fields.length

    if (isStandardLogInfo == 30) {
      //关键字段的index
      //val arr = Array(8, 9, 10, 12)

      //访客唯一标识和会员ID同时为-1，过滤
      if (fields(8).equals("-1") && fields(9).equals("-1")) {
        return ""
      }
      //会话ID过滤
      if (fields(10).equals("-1")) {
        return ""
      }
      //服务器时间过滤
      if (fields(12).equals("-1")) {
        return ""
      }

      //当前页面url 测试日志过滤
      if (fields(19).contains("http://127.0.0.1") || fields(19).contains("http://static.testmami.cn")){
        return  ""
      }

      //前一页面url 测试日志过滤
      if (fields(20).contains("http://127.0.0.1") || fields(20).contains("http://static.testmami.cn")){
        return  ""
      }

      var newLogline = ""

      //将时间转换成规定的格式
      if (fields(11) != "-1") {
        val oldClientTime = fields(11) //客户端时间
        val newClientTime = sdf_standard.format(sdf_origin.parse(oldClientTime))
        newLogline = logLine.replace(oldClientTime, newClientTime)
      }

      val oldServerTime = fields(12) //服务端时间
      val newServerTime = sdf_standard.format(sdf_origin.parse(oldServerTime))

      //调用方法获取服务端时间中的"小时"
      //      getHour(newServerTime)

      return newLogline.replace(oldServerTime, newServerTime)
    } else {
      return ""
    }
  }


  //提取服务端时间里的"小时"
  def getHour(serverTime: String): Unit = {

    //    println("进入getHour方法")
    var arrBuff = ArrayBuffer[Int]()

    val time = sdf_standard.parse(serverTime)

    val cal = Calendar.getInstance()

    cal.setTime(time)

    val hour = cal.get(Calendar.HOUR_OF_DAY)

    //将hour添加到arrBuff中
    var flag = true

    if (WebLogClean.arrBuff.length == 0) {
      WebLogClean.arrBuff += hour
    } else {
      for (i <- WebLogClean.arrBuff) {
        if (hour == i) {
          flag = false
        }
      }
      if (flag == true) {
        WebLogClean.arrBuff += hour
      }
    }
  }
}

object WebLogClean {

  var arrBuff = ArrayBuffer[Int]()

  def main(args: Array[String]) {

    //    val logFile = "hdfs://10.16.2.24:9000/flume/web_logs/"+WebLogClean.sdf_hdfsfolder.format(curDate)+"/*"
    //    val day = WebLogClean.sdf_hdfsfolder.format(curDate)
    //    val yesterday = "2018-03-29"
    //    val logFile = "hdfs://10.16.2.24:9000/xf/input/test-xf-0329.txt"
    //    val curDate = new Date()
    val webLogClean = new WebLogClean
    val logFile = s"${args(0)}"
    val yesterday = s"${args(1)}"
    val targetTableName = "ods_kunpu_mami_test_db.ods_mami_source_test"
    val TempViewName = "logs"

    //构建spark上下文
    val spark = SparkSession.builder()
      .appName("WebLogClean")
      //        .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    println("logFile===================:"+logFile)
    println("yesterday===================:"+yesterday)

    //读取hdfs上的日志文件
    val logFileSource = sc.textFile(logFile,1)

    //完成过滤
    val logLinesMapRDD = logFileSource.map(x => webLogClean.weblogParser(x)).filter(line => line != "")

    //关联case class
    val logLineClassRDD = logLinesMapRDD.map(line => line.split("\t")).map(x => Logs(x(0),x(1),x(2),x(3),x(4),x(5),
      x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(23),x(24)
      ,x(25),x(26),x(27),x(28),x(29),x(12).substring(0,10),x(12).substring(11,13)))


    import spark.sqlContext.implicits._
    logLineClassRDD.toDF().write.insertInto(targetTableName)








    //    logLinesMapRDD.cache()

    //    logLinesMapRDD.map(x => webLogClean.getHour(x.split("\t")(12)))


    /**
      * 1、过滤后的日志存储到hdfs上
      */
    //    logLinesMapRDD.saveAsTextFile("hdfs://10.16.2.24:9000/cleaned_logs/"+yesterday)


    /**
      * 2、过滤后的日志导入到hive原始分区表ods_mami_source_test中
      */






    //关联case class
    //    val logLineClassRDD = logLinesMapRDD.map(line => line.split("\t")).map(x => Log(x(0),x(1),x(2),x(3),x(4),x(5),
    //    x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(23),x(24),x(25),x(26),x(27),x(28),x(29)))
    //
    ////    logLineClassRDD.collect().foreach(println(_))
    //
    //    import spark.sqlContext.implicits._
    //    logLineClassRDD.toDF().createOrReplaceTempView(TempViewName)

    //    val arr = arrBuff.sorted.toArray

    //存到hive分区表
    //    Util.saveToHiveTable(spark, arr, yesterday, targetTableName, TempViewName)



    //    spark.sql("insert into table ods_kunpu_test_db.ods_mami_source_test partition(day='"+day+"',hour='17') select * from logs where serverTime between '2018-03-23-11:00:00' and '2018-03-23-12:00:00'").show()

    //加载到ods_cleaned_logs
    //    spark.sql("insert into table ods_cleaned_logs partition(day='"+day+"',hour='13') select * from logs")
    //
    //    spark.sql("select * from logs where serverTime between '2018-03-23-11:00:00' and '2018-03-23-12:00:00'").show()


    sc.stop()
  }
}
case class Logs(reqSerial:String,traceId:String,sysId:String,sysDesc:String,eventName:String,
                version:String,platformChannel:String,channelForm:String,deviceNum:String,customerId:String,
                sessionId:String,clientTime:String,serverTime:String,language:String,userAgent:String,
                resolution:String,ip:String,terminalChannel:String,domainId:String,currentUrl:String,
                previousUrl:String,title:String,orderId:String,productId:String,amount:String,
                category:String,action:String,duration:String,respCode:String,memo:String,day:String,hour:String)


case class Log(reqSerial:String,traceId:String,sysId:String,sysDesc:String,eventName:String,
               version:String,platformChannel:String,channelForm:String,deviceNum:String,customerId:String,
               sessionId:String,clientTime:String,serverTime:String,language:String,userAgent:String,
               resolution:String,ip:String,terminalChannel:String,domainId:String,currentUrl:String,
               previousUrl:String,title:String,orderId:String,productId:String,amount:String,
               category:String,action:String,duration:String,respCode:String,memo:String)








































