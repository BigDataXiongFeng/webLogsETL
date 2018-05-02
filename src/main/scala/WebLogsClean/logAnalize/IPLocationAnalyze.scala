package WebLogsClean.logAnalize

import java.util.{Date}
import org.apache.spark.sql.{SparkSession}



/**
  * 生产集群代码
  * 通过ip规则查找对应归属地(省、市)
  * 将规则广播变量到集群
  * 将结果保存到hive
  * xiongfeng
  * 2018/03/05
  */
object IpLocation {

  //将ip转化成十进制Long类型
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[. ]")
    var ipNum = 0L
    for(i <- 0 until(fragments.length)){
      ipNum = fragments(i).toLong  |  ipNum << 8L
    }
    ipNum
  }

  //自定义二分法查找ip在规则库中对应的index
  def searchIP(ip: Long, lines: Array[(String, String, String, String, String, String)]): Int = {
    var low = 0
    var high = lines.length-1
    while(low <= high){
      val middle = ( low + high ) / 2
      if((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong)){
        return middle
      }
      if(ip < lines(middle)._1.toLong){
        high = middle - 1
      }
      else {
        low = middle + 1
      }
    }
    -1
  }



  def main(args: Array[String]): Unit = {

    val curDate = new Date()
    println("开始时间"+curDate)
    //    val day = new SimpleDateFormat("yyyy-MM-dd").format(curDate)
    //    val yesterday = Util.getYesterday()
    //    val yesterday = "2018-04-02"
    val yesterday = s"${args(0)}"
    val sourceTableName = "ods_kunpu_mami_test_db.dwd_user_platform" //改成读dwd_user_platform去解析ip地址
    val targetTableName = "ods_kunpu_mami_test_db.dwd_user_area"
    val TempViewName = "userArea"

    //构建spark上下文
    val spark = SparkSession.builder()
      .appName("IPLocation")
      //      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    println("yesterday===================:"+yesterday)

    //读取IP规则文件
    val ipRulesRDD = sc.textFile("hdfs://10.16.2.24:9000/xf/input/ip_rules.txt").map(line => {
      val fields = line.split("\\|")
      val startNum = fields(2)              //获得IP段的起始值
      val endNum = fields(3)                //获得IP段的结束值
      val province = fields(6)              //获得IP段代表的省份
      val city = fields(7)                  //获得IP段代表的城市
      val area = fields(8)                  //获得IP段代表的区域
      val operator = fields(9)              //获取IP段代表的运营商
      (startNum, endNum, province, city, area, operator)
    })

    //driver端获得了ip归属地的规则
    val ipRulesArray = ipRulesRDD.collect()

    //driver端将规则广播出去到集群中每个节点
    val ipRulesBroadcast = sc.broadcast(ipRulesArray)

    //读ods_mami_source_test表获得DataFrame
    val sourceDF = spark.sql("select * from "+sourceTableName+" where day='"+yesterday+"'")

    //DF转换成RDD
    val sourceRDD = sourceDF.rdd


    //获取ip解析后的字段
    val provinceAndCityRDD = sourceRDD.map(fileds => {
      //      val fileds = line.toSeq
      val eventName = fileds(0).toString                    //事件名称
      val deviceNum = fileds(1).toString                    //访客唯一标识
      val customerId = fileds(2).toString                   //会员ID
      val sessionId = fileds(3).toString                    //会话ID
      val serverTime = fileds(4).toString                   //服务器时间
      val ip = fileds(5).toString                           //ip
      val app_channel = fileds(8).toString                  //产品
      val ipNum = ip2Long(ip)                               //ip转换
      val index = searchIP(ipNum,ipRulesBroadcast.value)    //下标

      //判断是否查询到ip
      var province = "-1"
      var city = "-1"
      var operator = "-1"
      if(index != -1){
        val info =ipRulesBroadcast.value(index)
        province = info._3
        if(info._4 != null){
          city = info._4
        }else{
          city = info._5
        }
        operator = info._6
      }

      //事件名称、访客唯一标识、会员ID、会话ID、服务器时间、用户ip、ip归属省份、ip归属城市、ip归属运营商、产品
      (eventName, deviceNum, customerId, sessionId, serverTime, ip, province, city, operator, app_channel)
    })

    //关联case class
    val ipClassRDD = provinceAndCityRDD.map(x => UserIpArea(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._5.substring(0,10),x._5.substring(11,13)))


    import spark.sqlContext.implicits._
    ipClassRDD.toDF().write.insertInto(targetTableName)







    //    //持久化
    //    provinceAndCityRDD.cache()
    //
    //    //获得partitions中的hour集合
    //    val timeDF = spark.sql("show partitions "+sourceTableName+" partition(day='"+yesterday+"')")
    //    val arr = Util.getHourList(timeDF)
    //
    //    //关联case class
    //    val ipClassRDD = provinceAndCityRDD.map(x => UserIpArea(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10))
    //
    //    ipClassRDD.collect().foreach(println(_))
    //
    //    //创建临时视图
    //    import spark.sqlContext.implicits._
    //    ipClassRDD.toDF().createOrReplaceTempView(TempViewName)
    //
    //    //加载到hive表中
    //    Util.saveToHiveTable(spark,arr,yesterday,targetTableName,TempViewName)
    println("结束时间" + new Date())
    spark.stop()
  }
}
case class IpArea(eventId: String, deviceNum: String, customerId: String, sessionId: String, serverTime: String,
                  ip: String, ipProvince: String, ipCity: String,operator: String, app_channel: String)

case class UserIpArea(eventId: String, deviceNum: String, customerId: String, sessionId: String, serverTime: String,
                      ip: String, ipProvince: String, ipCity: String,operator: String, app_channel: String,day: String,hour: String)