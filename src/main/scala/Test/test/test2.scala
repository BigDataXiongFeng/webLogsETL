package Test.test

import java.text.SimpleDateFormat
import java.util.Locale

import WebLogsClean.utils.Util
import org.apache.spark.sql.SparkSession

object test2 {

  val sdf_origin = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)
  val sdf_standard = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
  val sdf_hdfsfolder = new SimpleDateFormat("yy-MM-dd")


  def mysorted(a:String,b:String): Boolean = {

    test2.sdf_standard.format(test2.sdf_origin.parse(a.split("\t")(12))) < test2.sdf_standard.format(test2.sdf_origin.parse(b.split("\t")(12)))

  }


  //自定义方法获取基于session分析的指标
  def getVisitsInfo(logInfoGroup:List[String]):String = {

    //获取该次会话的访客唯一标识
    val deviceNum = logInfoGroup(0).split("\t")(8)

    //获取该次会话的会员Id
    val customerId = logInfoGroup(0).split("\t")(9)

    //获取该次会话的sessionID
    val sessionID = logInfoGroup(0).split("\t")(10)

    //获取该次会话的开始时间
    val startTime = logInfoGroup(0).split("\t")(12)

    //获取该次会话的结束时间
    val endTime = logInfoGroup(logInfoGroup.length-1).split("\t")(12)

    //获取该次会话第一次访问的url
    val entryPage = logInfoGroup(0).split("\t")(19)

    //获取该次会话最后一次访问的url
    val leavePage = logInfoGroup(logInfoGroup.length-1).split("\t")(19)

    //获取该次会话里所访问的页面总数
    //先用map函数将某次session里的所有访问记录变成(url,logInfo)元组的形式,然后再用groupBy函数按url分组,最后统计共有几个组
    val visitPageNum = logInfoGroup.map(log => (log.split("\t")(19),log)).groupBy(x => x._1).count(p => true)

    //访客唯一标识、会员ID、sessionID、会话起始时间、会话结束时间、进入页面url、离开页面url、访问页面数
    return deviceNum+" "+customerId+" "+sessionID+" "+startTime+" "+endTime+" "+entryPage+" "+leavePage+" "+visitPageNum

  }



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test2").master("local[2]").enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext

    val sourceTableName = "logs.ods_cleaned_logs"

    val targetTableName = "logs.xxx"

    val yesterday = Util.getYesterday()

    val timeDF = spark.sql("show partitions "+sourceTableName+" partition(day='"+yesterday+"')")

    val arr = Util.getHourList(timeDF)



    val sourceRDD = sc.textFile("hdfs://192.168.70.128:9000/input/dacs-access-for-session.log")

    val mapRDD = sourceRDD.map(line => (line.split("\t")(10),line)).groupByKey()

    val listRDD = mapRDD.map(_._2.toList.sortWith((a,b) => mysorted(a,b)))

    listRDD.foreach(println(_))

    Util.saveToHiveTable(spark,arr,yesterday,sourceTableName,targetTableName)

    spark.stop()

  }

}
