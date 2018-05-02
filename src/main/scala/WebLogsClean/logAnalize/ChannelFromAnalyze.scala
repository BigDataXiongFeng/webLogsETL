package WebLogsClean.logAnalize


import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession


/**
  * 生产集群代码
  * 日志中的渠道来源字段进行解析
  * 解析举例 01001014：安卓	速米袋	 豌豆荚
  * xiongfeng
  * 2018/03/12
  */
object channelFromAnalyze {


  //在码表总查找对应的渠道来源
  def searchChannelFrom(lines:Array[(String,String,String,String)], channelFrom: String):Tuple4[String,String,String,String] = {
    val t = new Tuple4("","","","")
    for(i <- (0 to lines.length - 1)){
      if(lines(i)._1.equals(channelFrom)){
        return lines(i)
      }
    }
    t
  }



  def main(args: Array[String]): Unit = {

    val curDate = new Date()
    println("开始时间"+curDate)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(curDate)
    //    val yesterday = Util.getYesterday()
    //    val yesterday = "2018-03-29"
    val yesterday = s"${args(0)}"
    val sourceTableName = "ods_kunpu_mami_test_db.ods_mami_source_test"
    val targetTableName = "ods_kunpu_mami_test_db.dwd_user_platform"
    val TempViewName = "userPlatform"

    //构建spark上下文
    val spark = SparkSession.builder()
      //      .master("local[2]")
      .appName("channelFromAnalyze")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    println("yesterday===================:"+yesterday)

    //获取平台渠道码表规则
    val channelFromRuleRDD = sc.textFile("hdfs://10.16.2.24:9000/xf/input/channelFrom_rules.txt").map(line => {
      val fields = line.split("\t")
      val channelFrom = fields(0)
      val app_channel = fields(1)
      val platform = fields(2)
      val traffic = fields(3)
      (channelFrom, platform, app_channel, traffic)
    })

    //diver端获得平台渠道码表规则,数组类型的
    val channelFromRuleArray = channelFromRuleRDD.collect()

    //广播变量
    val channelFromBroadcast = sc.broadcast(channelFromRuleArray)

    //读ods_mami_source_test表获得DataFrame
    val sourceDF = spark.sql("select * from "+sourceTableName+" where day='"+yesterday+"'")

    //DF转换成RDD
    val sourceRDD = sourceDF.rdd

    //根据channelFrom编码找到对应的规则
    val channelFromRDD = sourceRDD.map(fileds => {
      //      val fileds = line.split("\t")
      val eventName = fileds(4).toString         //事件名称
      val deviceNum = fileds(8).toString         //访客唯一标识
      val customerId = fileds(9).toString        //会员ID
      val sessionId = fileds(10).toString        //会话ID
      val serverTime = fileds(12).toString       //服务器时间
      val channelFrom = fileds(7).toString       //渠道来源
      val ip = fileds(16).toString               //ip

      var platform = ""
      var app_channel = ""
      var traffic = ""
      if(channelFrom == "-1"){
        platform = "-1"
        app_channel = "-1"
        traffic = "-1"
      }else{
        val channels = searchChannelFrom(channelFromBroadcast.value, channelFrom)
        platform = channels._2
        app_channel = channels._3
        traffic = channels._4
      }

      //事件名称、访客唯一标识、会员ID、会话ID、服务器时间、渠道来源、平台、app渠道、流量端
      (eventName,deviceNum,customerId,sessionId,serverTime,ip,channelFrom,platform,app_channel,traffic)
    })

    //关联case class
    val channelClass = channelFromRDD.map(x => UserPlatform(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._5.substring(0,10),x._5.substring(11,13)))


    import spark.sqlContext.implicits._
    channelClass.toDF().write.insertInto(targetTableName)





    //    //缓存
    //    channelFromRDD.cache()

    //获得partitions中的hour集合
    //    val timeDF = spark.sql("show partitions "+sourceTableName+" partition(day='"+yesterday+"')")
    //    val arr = Util.getHourList(timeDF)
    //
    //    //关联case class
    //    val channelClass = channelFromRDD.map(x => UserPlatform(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10))
    //
    //    channelClass.collect().foreach(println(_))
    //
    //    //创建临时视图
    //    import spark.sqlContext.implicits._
    //    channelClass.toDF().createOrReplaceTempView(TempViewName)
    //
    //    //加载到hive表
    //    Util.saveToHiveTable(spark,arr,yesterday,targetTableName,TempViewName)
    println("结束时间" + new Date())
    sc.stop()
  }
}

case class Platform(eventId: String,deviceNum: String,customerId: String,sessionId: String,serverTime: String,ip: String,
                    channelFrom: String,platform: String,app_channel: String,traffic: String)

case class UserPlatform(eventId: String,deviceNum: String,customerId: String,sessionId: String,serverTime: String,ip: String,
                        channelFrom: String,platform: String,app_channel: String,traffic: String,day: String,hour: String)