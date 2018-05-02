package Test.test

import WebLogsClean.logAnalize.IpLocation.{ip2Long, searchIP}
import org.apache.spark.sql.SparkSession

object test {

  //在码表查找对应的渠道来源
  def searchChannel(lines:Array[(String,String,String,String)], channelFrom: String):Tuple4[String,String,String,String] = {
    val t = new Tuple4("","","","")
    for(i <- (0 to lines.length - 1)){
      if(lines(i)._1.equals(channelFrom)){
        return lines(i)
      }
    }
    t
  }

  def main(args: Array[String]): Unit = {

    //构建spark上下文
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("channelFromAnalyze")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    //获取平台渠道码表规则
    val channelFromRuleRDD = sc.textFile("hdfs://192.168.70.128:9000/input/channelFrom_rules.txt").map(line => {
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

    val sourceRDD = sc.textFile("hdfs://192.168.70.128:9000/input/dacs-access0327.log")

    //根据channelFrom编码找到对应的规则
    val channelFromRDD = sourceRDD.map(line => {
      val fileds = line.split("\t")
      val eventName = fileds(4)        //事件名称
      val deviceNum = fileds(8)        //访客唯一标识
      val customerId = fileds(9)       //会员ID
      val sessionId = fileds(10)       //会话ID
      val serverTime = fileds(12)      //服务器时间
      val channelFrom = fileds(7)      //渠道来源
      val channels = searchChannel(channelFromBroadcast.value, channelFrom)


      //事件名称、访客唯一标识、会员ID、会话ID、服务器时间、渠道来源、平台、app渠道、流量端
      val channelLine = eventName +"\t"+ deviceNum +"\t"+ customerId +"\t"+ sessionId +"\t"+ serverTime +"\t"+ channels._2 +"\t"+ channels._3 +"\t"+ channels._4
      channelLine
    })

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

    channelFromRDD.map(line => {
      val fileds = line.split("\t")
      val ip = fileds(16)            //ip
      val ipNum = ip2Long(ip)
      val index = searchIP(ipNum,ipRulesBroadcast.value)
      val info =ipRulesBroadcast.value(index)
      val ipLine = line +"\t"+ info._3 +"\t"+ info._4 +"\t"+ info._5
      ipLine
    })



    //eventName +"\t"+ deviceNum +"\t"+ customerId +"\t"+ sessionId +"\t"+ serverTime +"\t"+ channels._2 +"\t"+ channels._3 +"\t"+ channels._4 +"\t"+ info._3 +"\t"+ info._4 +"\t"+ info._5



    sc.stop()

  }

}
