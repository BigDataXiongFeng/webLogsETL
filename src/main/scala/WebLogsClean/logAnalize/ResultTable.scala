package WebLogsClean.logAnalize

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * 生产集群代码
  * 从hive中间表中统计汇总数据
  * 得到各种结果表
  * 存入mysql中
  * xiongfeng
  * 2018-04-13
  */
object ResultTable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ReultTableToMysql")
      //      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    //构建配置
    val prop = new java.util.Properties()
    prop.setProperty("user","behavioral_opr")
    prop.setProperty("password","2ew6IPLK*O*O")


    //1、计算UV (今日用户数) = 今日访客数 + 今日会员数
    val DF1 = spark.sql(
      s"""
         |select
         |t0.visit_daliy + t1.customer_daliy as UV,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct deviceNum) visit_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where customerId = '-1'
         |and day = "${args(0)}"
         |) t0
         |,
         |(select
         |count(distinct customerId) customer_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where day = "${args(0)}"
         |and customerId != '-1'
         |) t1
      """.stripMargin)

    DF1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_user_visit_daliy_act",prop)


    //2、访客分析总表 dw_visitor_act_daliy_act
    //总活跃用户数,新增活跃用户数,日期
    val DF2 = spark.sql(
      s"""
         |select
         |t0.visit_daliy + t1.customer_daliy as uv_act,
         |t2.new_visit_daliy + t3.new_customer_daliy as uv_new,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct deviceNum) visit_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where customerId = '-1'
         |and day = "${args(0)}"
         |) t0
         |,
         |(select
         |count(distinct customerId) customer_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where day = "${args(0)}"
         |and customerId != '-1'
         |) t1
         |,
         |(select
         |count(distinct deviceNum) new_visit_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where customerId = '-1'
         |and eventName = 'e_l'
         |and day = "${args(0)}"
         |) t2
         |,
         |(select
         |count(distinct customerId) new_customer_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where eventName = 'e_l'
         |and day = "${args(0)}"
         |and customerId != '-1'
         |) t3
      """.stripMargin)

    DF2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_visitor_act_daliy_act",prop)



    //3、会员分析总表 dw_customer_act_daliy_act
    //总活跃会员数,新增活跃会员数,总会员数,日期
    val DF3 = spark.sql(
      s"""
         |select
         |t0.customer_daliy uv_act,
         |t1.new_customer_daliy uv_new,
         |t2.customer_all uv_all,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct customerId) customer_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where day = "${args(0)}"
         |and customerId != '-1'
         |) t0
         |,
         |(select
         |count(distinct customerId) new_customer_daliy
         |from
         |ods_kunpu_db.ods_mami_source ods
         |where eventName = 'e_l'
         |and day = "${args(0)}"
         |and customerId != '-1'
         |) t1
         |,
         |(select
         |count(distinct customerId) customer_all
         |from
         |ods_kunpu_db.ods_mami_source
         |where customerId != '-1'
         |) t2
        """.stripMargin)

    DF3.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_customer_act_daliy_act",prop)


    //4、dw_visitor_act_daliy_province
    //总活跃用户数,新增活跃用户数,省份,日期
    val DF4 = spark.sql(
      s"""
         |select
         |t0.visit_daliy + (case when t1.customer_daliy is null then 0 end) as uv_act,
         |(case when t2.new_visit_daliy is null then 0 end) + (case when t3.new_customer_daliy is null then 0 end) as uv_new,
         |t0.ipProvince province,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct deviceNum) visit_daliy,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where customerId = '-1'
         |and day = "${args(0)}"
         |group by ipProvince
         |) t0
         |left join
         |(select
         |count(distinct customerId) as customer_daliy,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by ipProvince
         |) t1
         |on t0.ipProvince = t1.ipProvince
         |left join
         |(select
         |count(distinct deviceNum) new_visit_daliy,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where customerId = '-1'
         |and eventId = 'e_l'
         |and day = "${args(0)}"
         |group by ipProvince
         |) t2
         |on t0.ipProvince = t2.ipProvince
         |left join
         |(select
         |count(distinct customerId) new_customer_daliy,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by ipProvince
         |) t3
         |on t0.ipProvince = t3.ipProvince
      """.stripMargin)

    DF4.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_visitor_act_daliy_province",prop)


    //5、dw_visitor_act_daliy_city
    //总活跃用户数,新增活跃用户数,省份,城市,日期
    val DF5 = spark.sql(
      s"""
         |select
         |t0.visit_daliy + (case when t1.customer_daliy is null then 0 end) as uv_act,
         |(case when t2.new_visit_daliy is null then 0 end) + (case when t3.new_customer_daliy is null then 0 end) as uv_new,
         |t0.ipProvince province,
         |t0.ipCity city,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct deviceNum) visit_daliy,ipProvince,ipCity
         |from
         |ods_kunpu_db.dwd_user_area
         |where customerId = '-1'
         |and day = "${args(0)}"
         |group by ipProvince,ipCity
         |) t0
         |left join
         |(select
         |count(distinct customerId) customer_daliy,ipProvince,ipCity
         |from
         |ods_kunpu_db.dwd_user_area
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by ipProvince,ipCity
         |) t1
         |on t0.ipCity = t1.ipCity
         |left join
         |(select
         |count(distinct deviceNum) new_visit_daliy,ipProvince,ipCity
         |from
         |ods_kunpu_db.dwd_user_area
         |where customerId = '-1'
         |and eventId = 'e_l'
         |and day = "${args(0)}"
         |group by ipProvince,ipCity
         |) t2
         |on t0.ipCity = t2.ipCity
         |left join
         |(select
         |count(distinct customerId) new_customer_daliy,ipProvince,ipCity
         |from
         |ods_kunpu_db.dwd_user_area
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by ipProvince,ipCity
         |) t3
         |on t0.ipCity = t3.ipCity
      """.stripMargin)

    DF5.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_visitor_act_daliy_city",prop)

    //6、新增、dw_visitor_act_daliy_app_channel
    //总活跃用户数,新增活跃用户数,产品,日期
    //    val DF6 = spark.sql(
    //          s"""
    //            |select
    //            |t0.visit_daliy + (case when t1.customer_daliy is null then 0 end) as uv_act,
    //            |(case when t2.new_visit_daliy is null then 0 end) + (case when t3.new_customer_daliy is null then 0 end) as uv_new,
    //            |t0.app_channel app_channel,
    //            |"${args(0)}" as day
    //            |from
    //            |(select
    //            |count(distinct deviceNum) visit_daliy,app_channel
    //            |from
    //            |ods_kunpu_db.dwd_user_area
    //            |where customerId = '-1'
    //            |and day = "${args(0)}"
    //            |group by app_channel
    //            |) t0
    //            |left join
    //            |(select
    //            |count(distinct customerId) customer_daliy,app_channel
    //            |from
    //            |ods_kunpu_db.dwd_user_area
    //            |where day = "${args(0)}"
    //            |and customerId != '-1'
    //            |group by app_channel
    //            |) t1
    //            |on t0.app_channel = t1.app_channel
    //            |left join
    //            |(select
    //            |count(distinct deviceNum) new_visit_daliy,app_channel
    //            |from
    //            |ods_kunpu_db.dwd_user_area
    //            |where customerId = '-1'
    //            |and eventId = 'e_l'
    //            |and day = "${args(0)}"
    //            |group by app_channel
    //            |) t2
    //            |on t0.app_channel = t2.app_channel
    //            |left join
    //            |(select
    //            |count(distinct customerId) new_customer_daliy,app_channel
    //            |from
    //            |ods_kunpu_db.dwd_user_area
    //            |where eventId = 'e_l'
    //            |and customerId != '-1'
    //            |and day = "${args(0)}"
    //            |group by app_channel
    //            |) t3
    //            |on t0.app_channel = t3.app_channel
    //          """.stripMargin)
    //
    //    DF6.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_visitor_act_daliy_app_channel",prop)


    //7、dw_customer_act_daliy_province
    //总活跃会员数,新增活跃会员数,总会员数,省份,日期
    val DF7 = spark.sql(
      s"""
         |select
         |t0.customer_all uv_act,
         |t1.new_customer_daliy uv_new,
         |t2.customer_daliy uv_all,
         |t0.ipProvince province,
         |"${args(0)}" as day
         |from
         |(
         |select
         |count(distinct customerId) customer_all,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where customerId != '-1'
         |group by ipProvince
         |) t0
         |left join
         |(
         |select
         |count(distinct customerId) new_customer_daliy,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by ipProvince
         |) t1
         |on t0.ipProvince = t1.ipProvince
         |left join
         |(
         |select
         |count(distinct customerId) customer_daliy,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by ipProvince
         |) t2
         |on t0.ipProvince = t2.ipProvince
        """.stripMargin)

    DF7.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_customer_act_daliy_province",prop)


    //8、dw_customer_act_daliy_city
    //总活跃会员数,新增活跃会员数,总会员数,省份,城市,日期
    val DF8 = spark.sql(
      s"""
         |select
         |t0.customer_all uv_act,
         |t1.new_customer_daliy uv_new,
         |t2.customer_daliy uv_all,
         |t0.ipProvince province,
         |t0.ipCity city,
         |"${args(0)}" day
         |from
         |(
         |select
         |count(distinct customerId) customer_all,ipCity,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where customerId != '-1'
         |group by ipProvince,ipCity
         |) t0
         |left join
         |(
         |select
         |count(distinct customerId) new_customer_daliy,ipCity,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by ipProvince,ipCity
         |) t1
         |on t0.ipCity = t1.ipCity
         |left join
         |(
         |select
         |count(distinct customerId) customer_daliy,ipCity,ipProvince
         |from
         |ods_kunpu_db.dwd_user_area
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by ipProvince,ipCity
         |) t2
         |on t0.ipCity = t2.ipCity
      """.stripMargin)

    DF8.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_customer_act_daliy_city",prop)



    //9、新增、dw_customer_act_daliy_app_channel
    //总活跃会员数,新增活跃会员数,总会员数,产品,日期
    //    val DF9 = spark.sql(
    //          s"""
    //            |select
    //            |t0.customer_all uv_act,
    //            |t1.new_customer_daliy uv_new,
    //            |t2.customer_daliy uv_all,
    //            |t0.app_channel app_channel,
    //            |"${args(0)}" as day
    //            |from
    //            |(
    //            |select
    //            |count(distinct customerId) customer_all,app_channel
    //            |from
    //            |ods_kunpu_db.dwd_user_area
    //            |where customerId != '-1'
    //            |group by app_channel
    //            |) t0
    //            |left join
    //            |(
    //            |select
    //            |count(distinct customerId) new_customer_daliy,app_channel
    //            |from
    //            |ods_kunpu_db.dwd_user_area
    //            |where eventId = 'e_l'
    //            |and customerId != '-1'
    //            |and day = "${args(0)}"
    //            |group by app_channel
    //            |) t1
    //            |on t0.app_channel = t1.app_channel
    //            |left join
    //            |(
    //            |select
    //            |count(distinct customerId) customer_daliy,app_channel
    //            |from
    //            |ods_kunpu_db.dwd_user_area
    //            |where day = "${args(0)}"
    //            |and customerId != '-1'
    //            |group by app_channel
    //            |) t2
    //            |on t0.app_channel = t2.app_channel
    //          """.stripMargin)
    //
    //    DF9.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_customer_act_daliy_app_channel",prop)


    //10、dw_visitor_act_daliy_platform
    //总活跃用户数,新增活跃用户数,平台,日期
    val DF10 = spark.sql(
      s"""
         |select
         |t0.visit_daliy + (case when t1.customer_daliy is null then 0 end) as uv_act,
         |(case when t2.new_visit_daliy is null then 0 end) + (case when t3.new_customer_daliy is null then 0 end) as uv_new,
         |t0.platform platform,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct deviceNum) visit_daliy,platform
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId = '-1'
         |and day = "${args(0)}"
         |group by platform
         |) t0
         |left join
         |(select
         |count(distinct customerId) customer_daliy,platform
         |from
         |ods_kunpu_db.dwd_user_platform
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by platform
         |) t1
         |on t0.platform = t1.platform
         |left join
         |(select
         |count(distinct deviceNum) new_visit_daliy,platform
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId = '-1'
         |and eventId = 'e_l'
         |and day = "${args(0)}"
         |group by platform
         |) t2
         |on t0.platform = t2.platform
         |left join
         |(select
         |count(distinct customerId) new_customer_daliy,platform
         |from
         |ods_kunpu_db.dwd_user_platform
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by platform
         |) t3
         |on t0.platform = t3.platform
      """.stripMargin)

    DF10.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_visitor_act_daliy_platform",prop)


    //11、dw_visitor_act_daliy_channel
    //总活跃用户数,新增活跃用户数,渠道,日期
    val DF11 = spark.sql(
      s"""
         |select
         |t0.visit_daliy + (case when t1.customer_daliy is null then 0 end) as uv_act,
         |(case when t2.new_visit_daliy is null then 0 end) + (case when t3.new_customer_daliy is null then 0 end) as uv_new,
         |t0.app_channel app_channel,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct deviceNum) visit_daliy,app_channel
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId = '-1'
         |and day = "${args(0)}"
         |group by app_channel
         |) t0
         |left join
         |(select
         |count(distinct customerId) customer_daliy,app_channel
         |from
         |ods_kunpu_db.dwd_user_platform
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by app_channel
         |) t1
         |on t0.app_channel = t1.app_channel
         |left join
         |(select
         |count(distinct deviceNum) new_visit_daliy,app_channel
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId = '-1'
         |and eventId = 'e_l'
         |and day = "${args(0)}"
         |group by app_channel
         |) t2
         |on t0.app_channel = t2.app_channel
         |left join
         |(select
         |count(distinct customerId) new_customer_daliy,app_channel
         |from
         |ods_kunpu_db.dwd_user_platform
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by app_channel
         |) t3
         |on t0.app_channel = t3.app_channel
      """.stripMargin)

    DF11.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_visitor_act_daliy_channel",prop)


    //12、dw_visitor_act_daliy_traffic
    //总活跃用户数,新增活跃用户数,流量端,日期
    val DF12 = spark.sql(
      s"""
         |select
         |t0.visit_daliy + (case when t1.customer_daliy is null then 0 end) as uv_act,
         |(case when t2.new_visit_daliy is null then 0 end) + (case when t3.new_customer_daliy is null then 0 end) as uv_new,
         |t0.traffic traffic,
         |"${args(0)}" as day
         |from
         |(select
         |count(distinct deviceNum) visit_daliy,traffic
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId = '-1'
         |and day = "${args(0)}"
         |group by traffic
         |) t0
         |left join
         |(select
         |count(distinct customerId) customer_daliy,traffic
         |from
         |ods_kunpu_db.dwd_user_platform
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by traffic
         |) t1
         |on t0.traffic = t1.traffic
         |left join
         |(select
         |count(distinct deviceNum) new_visit_daliy,traffic
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId = '-1'
         |and eventId = 'e_l'
         |and day = "${args(0)}"
         |group by traffic
         |) t2
         |on t0.traffic = t2.traffic
         |left join
         |(select
         |count(distinct customerId) new_customer_daliy,traffic
         |from
         |ods_kunpu_db.dwd_user_platform
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by traffic
         |) t3
         |on t0.traffic = t3.traffic
      """.stripMargin)

    DF12.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_visitor_act_daliy_traffic",prop)


    //13、dw_customer_act_daliy_platform
    //总活跃会员数,新增活跃会员数,总会员数,平台,日期
    val DF13 = spark.sql(
      s"""
         |select
         |t0.customer_all uv_act,
         |t1.new_customer_daliy uv_new,
         |t2.customer_daliy uv_all,
         |t0.platform platform,
         |"${args(0)}" as day
         |from
         |(
         |select
         |count(distinct customerId) customer_all,platform
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId != '-1'
         |group by platform
         |) t0
         |left join
         |(
         |select
         |count(distinct customerId) new_customer_daliy,platform
         |from
         |ods_kunpu_db.dwd_user_platform
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by platform
         |) t1
         |on t0.platform = t1.platform
         |left join
         |(
         |select
         |count(distinct customerId) customer_daliy,platform
         |from
         |ods_kunpu_db.dwd_user_platform
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by platform
         |) t2
         |on t0.platform = t2.platform
      """.stripMargin)

    DF13.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_customer_act_daliy_platform",prop)


    //14、dw_customer_act_daliy_channel
    //总活跃会员数,新增活跃会员数,总会员数,渠道,日期
    val DF14 = spark.sql(
      s"""
         |select
         |t0.customer_all uv_act,
         |t1.new_customer_daliy uv_new,
         |t2.customer_daliy uv_all,
         |t0.app_channel app_channel,
         |"${args(0)}" as day
         |from
         |(
         |select
         |count(distinct customerId) customer_all,app_channel
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId != '-1'
         |group by app_channel
         |) t0
         |left join
         |(
         |select
         |count(distinct customerId) new_customer_daliy,app_channel
         |from
         |ods_kunpu_db.dwd_user_platform
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by app_channel
         |) t1
         |on t0.app_channel = t1.app_channel
         |left join
         |(
         |select
         |count(distinct customerId) customer_daliy,app_channel
         |from
         |ods_kunpu_db.dwd_user_platform
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by app_channel
         |) t2
         |on t0.app_channel = t2.app_channel
      """.stripMargin)

    DF14.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_customer_act_daliy_channel",prop)


    //15、dw_customer_act_daliy_traffic
    //总活跃会员数,新增活跃会员数,总会员数,流量端,日期
    val DF15 = spark.sql(
      s"""
         |select
         |t0.customer_all uv_act,
         |t1.new_customer_daliy uv_new,
         |t2.customer_daliy uv_all,
         |t0.traffic traffic,
         |"${args(0)}" as day
         |from
         |(
         |select
         |count(distinct customerId) customer_all,traffic
         |from
         |ods_kunpu_db.dwd_user_platform
         |where customerId != '-1'
         |group by traffic
         |) t0
         |left join
         |(
         |select
         |count(distinct customerId) new_customer_daliy,traffic
         |from
         |ods_kunpu_db.dwd_user_platform
         |where eventId = 'e_l'
         |and customerId != '-1'
         |and day = "${args(0)}"
         |group by traffic
         |) t1
         |on t0.traffic = t1.traffic
         |left join
         |(
         |select
         |count(distinct customerId) customer_daliy,traffic
         |from
         |ods_kunpu_db.dwd_user_platform
         |where day = "${args(0)}"
         |and customerId != '-1'
         |group by traffic
         |) t2
         |on t0.traffic = t2.traffic
      """.stripMargin)

    DF15.write.mode(SaveMode.Append).jdbc("jdbc:mysql://rm-uf62k16sfl26h96ly.mysql.rds.aliyuncs.com:3306/","behavioral_db.dw_customer_act_daliy_traffic",prop)

    spark.stop()

  }
}
