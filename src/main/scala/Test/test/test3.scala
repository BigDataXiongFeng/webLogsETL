package Test.test

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  */
object test3 {

  def main(args: Array[String]): Unit = {

    //构建spark上下文
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("channelFromAnalyze")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql(
      """
        |select
        |count(distinct customerId) customer_all,ipCity,ipProvince
        |from
        |ods_kunpu_test_db.dwd_user_area
        |group by ipProvince,ipCity
      """.stripMargin)

//    val sqlContext = new SQLContext(spark.sparkContext)



    val prop = new Properties()

    prop.setProperty("user","root")

    prop.setProperty("password","xiongfeng")

    val df1 = spark.read.jdbc("jdbc:mysql://localhost:3306","xiongfeng.user",prop)

    df1.show()

    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306","xiongfeng.user",prop)

    println("finish")

    spark.stop()

  }
}
