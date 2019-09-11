package com.qiao

import javax.xml.xpath.{XPath, XPathConstants, XPathFactory}
import okhttp3.{FormBody, OkHttpClient, Request, Response}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.types.IntegerType
import org.htmlcleaner.{CleanerProperties, DomSerializer, HtmlCleaner, TagNode}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.w3c.dom.Document
import org.apache.spark.sql.functions._
import scala.collection.mutable

object Test {

  val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  spark.udf.register("xpathtostring",(text: String, path: String) => {
    if (StringUtils.isNotBlank(text) && text.length > 5) {
      if (text.startsWith("[\"<tbody")) {
        text.substring(0, 2) + "<table>" + text.substring(3)
      }
      val result = getValueByXpath(text, path)
      result
    } else {
      ""
    }
  })
  spark.udf.register("type_udf",(x: String) => {
    if (x == None || x.isInstanceOf[Int] || x == "") "_"
    else if (x.contains("有限责任") || x.contains("分公司") || x.contains("内资")) "有限责任公司"
    else if (x.contains("股份")) "股份有限公司"
    else if (x.contains("集体") || x.contains("公民") || x.contains("合作社") || x.contains("全民")) "集体所有制"
    else if (x.contains("国有") || x.contains("办事处") || x.contains("事业单位")) "国有企业"
    else if (x.contains("个体") || x.contains("个人经营")) "个体工商户"
    else if (x.contains("个人独资")) "个人独资企业"
    else if (x.contains("有限合伙") || x.contains("农村资金互助")) "有限合伙"
    else if (x.contains("普通合伙") || x.contains("农民")) "普通合伙"
    else if (x.contains("外商投资") || x.contains("外国") || x.contains("外资")) "外商投资企业"
    else if (x.contains("港") || x.contains("澳") || x.contains("台")) "港,澳,台"
    else if (x.contains("联营")) "联营企业"
    else if (x.contains("私营")) "私营企业"
    else if (x.contains("合伙企业") || x.contains("经营单位")) "有限合伙"
    else x
  })
  spark.udf.register("employe_udf",(x: String) => {
    if (x == "") "_"
    else if (x.toInt >= 0 && x.toInt <= 49) "0-49人"
    else if (x.toInt >= 50 && x.toInt <= 99) "50-99人"
    else if (x.toInt >= 100 && x.toInt <= 499) "100-499人"
    else if (x.toInt >= 500 && x.toInt <= 999) "500-999人"
    else if (x.toInt >= 1000 && x.toInt <= 4999) "1000-4999人"
    else if (x.toInt >= 5000 && x.toInt <= 9999) "5000-9999人"
    else "10000以上"
  })
  spark.udf.register("exchange_udf" , (x1: String, x2: String) => {
    if (x1 == None) {
      0.toDouble
    } else if (x2 == None) {
      0.toDouble
    } else {
      try {
        if (x1.contains("美元")) {
          x2.toFloat * 6.908
        } else if (x1.contains("欧元")) {
          x2.toFloat * 7.7107
        } else if (x1.contains("日")) {
          x2.toFloat * 0.06252
        } else if (x1.contains("港")) {
          x2.toFloat * 0.8801
        } else if (x1.contains("英")) {
          x2.toFloat * 8.7842
        } else if (x1.contains("澳")) {
          x2.toFloat * 4.7527
        } else if (x1.contains("法")) {
          x2.toFloat * 6.8315
        } else if (x1.contains("新")) {
          x2.toFloat * 5.0120
        } else if (x1.contains("拿")) {
          x2.toFloat * 5.1545
        } else {
          0.toDouble
        }
      } catch {
        case e: Exception => 0.toDouble
      }
    }
  })

  spark.udf.register( "trans_udf",(x: String) => {
    val pattern = "\\d+\\.?\\d".r
    pattern.findAllIn(x).next().toString()
  })

  spark.udf.register( "null_udf" , (text:String) =>{
    if (text.startsWith("www")) {
      text
    } else {
      "_"
    }
  })

  spark.udf.register("cpca_udf" , (text:String , num:Int) =>{
    if (StringUtils.isNotBlank(text) && text.length > 0) {
      val client = new OkHttpClient()
      val formBodyBuilder = new FormBody.Builder
      val url: String = "http://localhost:8000/parse/"
      val body: FormBody = formBodyBuilder.add("地区", text).build()
      val request: Request = new Request.Builder()
        .url(url)
        .post(body)
        .build()
      val response: Response = client.newCall(request).execute()
      response.body().string().split("/")(num)
    } else {
      ""
    }
  })

  spark.udf.register( "vc_udf" , (vc_time: String, vc_amount: String, vc_step: String, vc_valuation: String, vc_proportion: String, vc_investor: String) =>{
    val map = scala.collection.mutable.Map[String, String]()
    map.put("date", vc_time)
    map.put("amount", vc_amount)
    map.put("step", vc_step)
    map.put("valuation", vc_valuation)
    map.put("proportion", vc_proportion)
    map.put("investor", vc_investor)

    val map2 = scala.collection.mutable.Map[String, mutable.Map[String, String]]()
    map2.put("vc", map)
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.write(map2)
  })

  def getValueByXpath(html: String, xPath: String): String = {
    val tagNode: TagNode = new HtmlCleaner().clean(html)
    var value: String = null
    try {
      val doc: Document = new DomSerializer(new CleanerProperties).createDOM(tagNode)
      val xpath: XPath = XPathFactory.newInstance.newXPath
      value = xpath.evaluate(xPath, doc, XPathConstants.STRING).asInstanceOf[String]
    } catch {
      case e: Exception =>
        println("Extract value error. " + e.getMessage)
        e.printStackTrace()
    }
    value
  }


  def main(args: Array[String]): Unit = {


    val readDF: DataFrame = spark.read.text("data/tianyan/input")


    val jsonDF: Dataset[Row] = readDF.select(col("value"),
      get_json_object(col("value"), "$.run_spider_time").alias("time"),
      get_json_object(col("value"), "$.company_url").alias("company_href"),
      get_json_object(col("value"), "$.company_name").alias("company_name"),
      get_json_object(col("value"), "$.basic_info").alias("html_basic_info"),
      get_json_object(col("value"), "$.business_info").alias("html_business_info"),
      get_json_object(col("value"), "$.legal_persion").alias("html_legal_persion"),
      get_json_object(col("value"), "$.shareholder").alias("html_shareholder"),
      get_json_object(col("value"), "$.financing").alias("html_financing")
    )
    jsonDF.filter(col("time").cast(IntegerType) > 1565366600.674059)
    jsonDF.select(col("html_basic_info")).show(1)
    jsonDF.createOrReplaceTempView("distinct_data")
    val parsedDF = spark.sql("select xpathtostring(html_basic_info,'/html/body/div[3]/div[1]/h1/text()') as company_name," +
      "xpathtostring(html_basic_info,'/html/body/div[3]/div[2]/div/span[1]/text()') as company_status," +
      "xpathtostring(html_business_info,'//tbody/tr[3]/td[2]//text()') as company_number," +
      "xpathtostring(html_basic_info,'/html/body/div[5]/span[2]/text()') as phone," +
      "xpathtostring(html_basic_info,'//*[@class=\"in-block sup-ie-company-header-child-2\"]/span[2]/text()') as email," +
      "xpathtostring(html_basic_info,'//*[@class=\"in-block sup-ie-company-header-child-2\"]/div/div/text()') as address," +
      "xpathtostring(html_basic_info,'//*[@class=\"summary\"]/div/div/text()') as introduce," +
      "xpathtostring(html_business_info,'//tbody/tr[10]/td[2]/text()') as registered_address," +
      "xpathtostring(html_business_info,'//tbody/tr[5]/td[2]/text()') as company_type," +
      "xpathtostring(html_business_info,'//tbody/tr[5]/td[4]/text()') as industry," +
      "xpathtostring(html_business_info,'//tbody/tr[11]/td[2]') as business_scope," +
      "xpathtostring(html_business_info,'//tbody/tr[1]/td[2]/div/text()') as registered_capital," +
      "xpathtostring(html_business_info,'//tbody/tr[8]/td[2]/text()') as employe_number," +
      "xpathtostring(html_business_info,'//tbody/tr[5]/td[2]/span/text()') as period," +
      "xpathtostring(html_business_info,'//div[2]/text()') as registered_time," +
      "xpathtostring(html_business_info,'//tbody/tr[7]/td[@colspan=\"2\"]/text()') as employe_scale," +
      "xpathtostring(html_financing,'/html/body/table/tbody//tr/td[2]/text()') as vc_time," +
      "xpathtostring(html_financing,'/html/body/table/tbody//tr/td[4]/text()') as vc_amount," +
      "xpathtostring(html_financing,'/html/body/table/tbody//tr/td[5]/text()') as vc_step," +
      "xpathtostring(html_financing,'/html/body/table/tbody//tr/td[6]/text()') as vc_valuation," +
      "xpathtostring(html_financing,'/html/body/table/tbody//tr/td[7]/text()') as vc_proportion," +
      "xpathtostring(html_financing,'/html/body/table/tbody/tr[1]/td[8]/div/a/text()') as vc_investor," +
      "xpathtostring(html_basic_info,'/html/body/div[3]/div[3]/div[2]/div[1]/a/text()') as website," +
      "xpathtostring(html_legal_persion,'/html/body/div[1]/div[1]/div[2]/div[1]/a/text()') as legal_name," +
      "xpathtostring(html_legal_persion,'/html/body/div[1]/div[2]/div/div[2]/span/text()') as incorporated_company," +
      "xpathtostring(html_business_info,'//tbody/tr[1]/td[4]/text()') as contributed_capital," +
      "xpathtostring(html_business_info,'/html/body/img[1]/@alt') as score," +
      "xpathtostring(html_business_info,'//tbody/tr[3]/td[4]/text()') as business_registration_number," +
      "xpathtostring(html_business_info,'//tbody/tr[4]/td[2]/text()') as identification_number," +
      "xpathtostring(html_business_info,'//tbody/tr[6]/td[2]/text()') as taxpayer_qualification," +
      "xpathtostring(html_business_info,'//tbody/tr[9]/td[@colspan=\"2\"]/text()') as english_name," +
      "xpathtostring(html_shareholder,'//*[@id=\"_container_holder\"]/table/tbody/tr[1]/td[2]/table/tbody/tr/td[2]/a/text()') as shareholders_information from distinct_data")
    //   parsedDF.createOrReplaceTempView("parsedDF")
    parsedDF.show()
  }
}
