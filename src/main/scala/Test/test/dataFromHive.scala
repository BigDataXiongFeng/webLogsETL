package Test.test

import org.apache.spark.sql.SparkSession


/**
  * 测试spark读写hive操作
  * 将配置文件（core-site.xml,hdfs-site.xml,hive-site.xml）放置于resource目录下
  */
object dataFromHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("dataFromHive")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    //可以设置运行参数
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")

    //也可以使用Scala的迭代器来读取configMap中的数据
    //val configMap:Map[String, String] = spark.conf.getAll()
    //spark.catalog.listDatabases.show(false)

    //从hive中读取数据
    spark.sql("select * from web_logs.spark_stud_info").show()

    //创建表格到hive
    spark.sql("CREATE TABLE IF NOT EXISTS  web_logs.people (name String, age Int) USING hive")

    //加载数据到hive表（源数据文件放置于resource目录下）
    spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/people.txt' INTO TABLE  web_logs.people")

    spark.sql("select * from  web_logs.people").show()

    spark.sql("select * from logs.ods_cleaned_logs").show()


    spark.stop()


  }


}
