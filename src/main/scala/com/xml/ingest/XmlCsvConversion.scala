package com.xml.ingest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.collection.immutable.ListMap

object XmlCsvConversion  {
  var currentRec = scala.collection.immutable.Map[String, Any]()
  var prevRec = scala.collection.immutable.Map[String, Any]()
  var orderMap = scala.collection.immutable.Map[String, Int]()
  var count = 1
  var finalData = List[Map[String, Any]]()
  var finalOrderMap = List[Map[String, Int]]()
  var elementList = List[String]()
  var inputdataMap = scala.collection.mutable.Map[String, String]()
  var currentRefRec = scala.collection.immutable.Map[String, Any]()

  def main(args: Array[String]) {
    val table_name: String = (args(0)).trim()
   
    pivotConversion(table_name)

  }

  def pivotConversion(table_name: String): Unit = {
    try {

      val conf = new SparkConf()
        .setAppName("log")
      val sc = new SparkContext(conf)
      val spark = SparkSession.builder()
        .appName("log")
        .enableHiveSupport()
        .getOrCreate()

      var header_new = List[String]()
      var df = spark.sql("SELECT * FROM " + table_name)

      df = df.withColumn("idx", monotonically_increasing_id())

      var df1 = spark.sql("SELECT * FROM " + table_name + "_lookup")

      val colName1 = df1.schema.fieldNames(0)
      val colName2 = df1.schema.fieldNames(1)

      var rows1 = df1.collect()
      rows1.foreach(currentRecord => refrence(currentRecord, colName1, colName2))

      df.collect().foreach(row => pivot(row, colName1, colName2))

      var sortedMap = ListMap(orderMap.toSeq.sortWith(_._2 < _._2): _*)
      currentRec.foreach {
        case (k, v) =>
          if (inputdataMap.contains(k)) {
            currentRefRec += inputdataMap.getOrElse(k, "") -> v
          } else {
            currentRefRec += k -> v
          }
      }
      finalData = currentRefRec :: finalData
      finalOrderMap = sortedMap :: finalOrderMap
      currentRec = scala.collection.immutable.Map[String, Any]()

      val headers = finalOrderMap.flatMap(entry => entry.keySet).distinct

      for (i <- headers) {

        i.replaceAll(":", "_")
        header_new = header_new :+ i.replaceAll("\\:", "_")

      }

      val csvData = finalData.map(entry => headers.map(column => entry.get(column).getOrElse(0)).mkString(",")).mkString("\n")
      val csvData1 = finalData.map(entry => headers.map(column => entry.getOrElse(column, "")))
      val rdd1 = sc.parallelize(csvData1)
      val RowRDD = rdd1.map(r => Row.fromSeq(r))
      val schema = StructType(header_new.toSeq.map(StructField(_, StringType)))
      val outputDf = spark.createDataFrame(RowRDD, schema)

      outputDf.show()
      outputDf.write.mode("overwrite").saveAsTable(table_name + "_pivot")
    } catch {
      case unknown: Exception => {
        println("Unknown exception" + unknown.getMessage)
        throw new Exception(unknown.getMessage);
      }
    }
  }

  def pivot(currentRecord: org.apache.spark.sql.Row, colName1: String, colName2: String) {
    val currentRecordMap = currentRecord.schema.fieldNames.map(field => field -> currentRecord.getAs[String](field)).toMap

    if (currentRec.contains(currentRecordMap(colName1))) {
      currentRec.foreach {
        case (k, v) =>
          if (inputdataMap.contains(k)) {
            currentRefRec += inputdataMap.getOrElse(k, "") -> v
          } else {
            currentRefRec += k -> v
          }
      }
      finalData = currentRefRec :: finalData
      prevRec = currentRec
      currentRec = scala.collection.immutable.Map[String, Any]()

      val n = elementList.length - elementList.indexOf(currentRecordMap(colName1))

      elementList = elementList.patch(elementList.indexOf(currentRecordMap(colName1)), Nil, n)

      elementList.foreach(key => {
        currentRec += key -> prevRec(key)
      })
      currentRec += currentRecordMap(colName1) -> currentRecordMap(colName2)

    } else {
      currentRec += currentRecordMap(colName1) -> currentRecordMap(colName2)
    }

    elementList = elementList :+ currentRecordMap(colName1)

  }

  def refrence(currentRecord: org.apache.spark.sql.Row, colName1: String, colName2: String) {
    inputdataMap += currentRecord.getAs[String](colName1) -> currentRecord.getAs[String](colName2)
    orderMap += currentRecord.getAs[String](colName2) -> count
    count = count + 1
  }

}






