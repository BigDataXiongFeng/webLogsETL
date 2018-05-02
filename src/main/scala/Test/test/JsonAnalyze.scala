package Test.test

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession


object JsonAnalyze {

  case class report(data_type:String,source_name:String,source_name_zh:String,data_gain_time:String,task_id:String,update_time:String,version:String)


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
        .appName("JsonAnalyze")
        .master("local[2]")
        .getOrCreate()

    val sc = spark.sparkContext


//
//    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val source1 = sc.textFile("file:///Users/xiongfeng/Desktop/aa.json")

//    source.collect().foreach(x => print(x + "\\$"))
//    println("=====1=====")
//    val first = source.take(2)(0)
//    println("=====2=====")
//    println("first是："+first)
//    println("first.getClass是："+first.getClass)
//    val p = parse(first).extract[Person]
//    println(p.name)
//    println("==========")
//    input.collect().foreach(x => {var c = parse(x).extract[Person];println(c.name + "," + c.age)})



    val df = spark.read.json("/Users/xiongfeng/Desktop/aa.json")



//    val s = JSON.toJSONString(df.toJSON)

//    df.toJSON.printSchema()

    val source = "{\n  \"report\": [\n    {\n      \"key\": \"data_type\",\n      \"value\": \"运营商\"\n    },\n    {\n      \"key\": \"source_name\",\n      \"value\": \"chinatelecomsh\"\n    },\n    {\n      \"key\": \"source_name_zh\",\n      \"value\": \"上海电信\"\n    },\n    {\n      \"key\": \"data_gain_time\",\n      \"value\": \"2018-03-23\"\n    },\n    {\n      \"key\": \"task_id\",\n      \"value\": \"29c73570-2e51-11e8-8adf-00163e0f4b67\"\n    },\n    {\n      \"key\": \"update_time\",\n      \"value\": \"2018-03-23 12:17:56\"\n    },\n    {\n      \"key\": \"version\",\n      \"value\": \"1.0\"\n    }\n  ],\n  \"user_basic\": [\n    {\n      \"key\": \"name\",\n      \"value\": \"汤国庆\"\n    },\n    {\n      \"key\": \"id_card\",\n      \"value\": \"310228198009282611\"\n    },\n    {\n      \"key\": \"gender\",\n      \"value\": \"男\"\n    },\n    {\n      \"key\": \"age\",\n      \"value\": \"37\"\n    },\n    {\n      \"key\": \"constellation\",\n      \"value\": \"天秤座\"\n    },\n    {\n      \"key\": \"province\",\n      \"value\": \"上海市\"\n    },\n    {\n      \"key\": \"city\",\n      \"value\": \"上海市\"\n    },\n    {\n      \"key\": \"region\",\n      \"value\": \"金山县\"\n    },\n    {\n      \"key\": \"native_place\",\n      \"value\": \"上海市金山县\"\n    }\n  ],\n  \"cell_phone\": [\n    {\n      \"key\": \"mobile\",\n      \"value\": \"18017422626\"\n    },\n    {\n      \"key\": \"carrier_name\",\n      \"value\": \"汤国庆\"\n    },\n    {\n      \"key\": \"carrier_idcard\",\n      \"value\": \"310228***********1\"\n    },\n    {\n      \"key\": \"reg_time\",\n      \"value\": \"2012-02-19 00:00:00\"\n    },\n    {\n      \"key\": \"in_time\",\n      \"value\": \"74\"\n    },\n    {\n      \"key\": \"email\",\n      \"value\": \"运营商未提供邮箱\"\n    },\n    {\n      \"key\": \"address\",\n      \"value\": \"金山枫泾镇枫阳新村**7号*层**1室\"\n    },\n    {\n      \"key\": \"reliability\",\n      \"value\": \"实名认证\"\n    },\n    {\n      \"key\": \"phone_attribution\",\n      \"value\": \"上海\"\n    },\n    {\n      \"key\": \"live_address\",\n      \"value\": \"上海\"\n    },\n    {\n      \"key\": \"available_balance\",\n      \"value\": \"60\"\n    },\n    {\n      \"key\": \"package_name\",\n      \"value\": \"十全十美不限量129元套餐（乐享家）\"\n    },\n    {\n      \"key\": \"bill_certification_day\",\n      \"value\": \"2018-03-31\"\n    }\n  ]}"

    val json = JSON.parseObject(source.toString)
    val report = json.getJSONArray("report")
    for(i <- 0 to report.size()-1){
      val jo = report.get(i)
      print(JSON.parseObject(jo.toString).get("key"))
      print("==")
      println(JSON.parseObject(jo.toString).get("value"))
    }

//    println(json.get("name"))



    //显示json数据
//    source.show()
//
//    println("a")
//
//    //显示json结构
//    source.printSchema()




  }

}
