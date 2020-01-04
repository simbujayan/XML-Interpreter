package com.xml.ingest
import scala.xml._
/*import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf*/
import java.text.SimpleDateFormat
import java.util.Date
/*import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkFiles*/
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
/*import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset*/

object XMLClusterFinal {
 
   val file = new File("C:/myspace/athena.txt" )
   val bw = new BufferedWriter(new FileWriter(file))
 // case class MapObj(key: String, value: String)
  var keyValue = scala.collection.mutable.Map[String, String]()
  
  def extractText(nodes: Seq[xml.Node]): Seq[String] =  nodes.flatMap 
  {
   case xml.Text(t) => Seq(t)
   case n => extractText(n.child)
  }
   
   /*extractText(xml.XML.loadString("""C:/myspace/XML/XML/clinic.xml"""))
  .filter(_.matches(".*\\S.*"))
  .mkString("\n")*/
  def linearize(node: Node, stack: String, map: Map[String, String]): List[(Node, String, Map[String, String])] = {
     (node, stack, map) :: node.child.flatMap {
     
      case e: Elem => {
 
         if (e.descendant.size == 1) {
           //var sparkObj = spark
            if(e.attributes.toList.size>=1)
          {           
            e.attributes.toList.foreach{f =>
            println("Key:"+f.key+" Value:"+f.value)
            println("Attrib:"+stack + ":" + e.label +":"+f.key->f.value)
            var key = stack + ":" + e.label +":"+f.key
            var value = f.value
            keyValue +=(key->value.toString())
             bw.write(stack + ":" + e.label +":"+f.key+","+f.value.toString()+'\n')
             //sparkObj.sqlContext.sql("insert into table sample.xmltoKeyValue values ('"+stack + ":" + e.label +":" + f.key+"','"+f.value.toString()+"')").show()
            //linearize(e, stack, map ++ Map(stack + ":" + e.label+":"+ f.key-> f.value.toString()))
            }
 
          }
              println("The value of e.text is " +stack + ":" + e.label -> e.text)
              keyValue  += (stack + ":" + e.label -> e.text)
              bw.write(stack + ":" + e.label+","+ e.text+'\n')
              //sparkObj.sqlContext.sql("insert into table sample.xmltoKeyValue values ('"+stack + ":" + e.label+"','"+e.text+"')").show()
              linearize(e, stack, map ++ Map(stack + ":" + e.label -> e.text))
          
        }
         else if(e.attributes!=null && e.attributes.toList.size>=1)
         {
            //var sparkObj = spark
            println("Attrib:"+stack + ":" + e.label +":"+e.attributes+" = "+e.attributes.toList.size)
            /*println("Element text:"+e.text)
            bw.write(stack + ":" + e.label+","+e.text+'\n')*/
            var key = ""
            var value = ""
            e.attributes.toList.foreach{f =>
            println("Key:"+f.key+" Value:"+f.value)
            bw.write(stack + ":" + e.label +":" + f.key+","+f.value.toString()+'\n')
            keyValue +=(stack + ":" + e.label +":" + f.key->f.value.toString())
            //sparkObj.sqlContext.sql("insert into table sample.xmltoKeyValue values ('"+stack + ":" + e.label +":" + f.key+"','"+f.value.toString()+"')").show()
            //linearize(spark,e, stack, map ++ Map(stack + ":" + e.label+":"+ f.key-> f.value.toString()))
            }
             linearize(e, stack + ":" + e.label, map)
         }
     
        else if(e.attributes.isEmpty && (e.child.size == 0) )
        {
           //var sparkObj = spark
           println("self tag:"+stack + ":" + e.label  -> null)
           bw.write(stack + ":" + e.label+","+null+'\n')
           keyValue +=(stack + ":" + e.label  -> null)
           //sparkObj.sqlContext.sql("insert into table sample.xmltoKeyValue values ('"+stack + ":" + e.label+"','"+null+"')").show()
           linearize(e, stack, map ++ Map(stack + ":" + e.label  -> null))
        }
         
        else linearize(e, stack + ":" + e.label, map)
      }        
      case _ => Nil
    }.toList

  }

  def main(args: Array[String]) {
   // var path: String = (args(0)).trim()
    val xmlFile = XML.load("""C:/myspace/XML/XML/athena.xml""").toString();
    //val xmlFile = XML.load(path).toString()
    val result: Elem = scala.xml.XML.loadString(xmlFile)
    
     // Spark Submission
       /* val warhouselocation = "/user/hive/warehouse";
         val spark = SparkSession
        .builder()
       //.master("local[*]")
        .appName("Spark Hive Example")
        .config("spark.sql.warehouse.dir",warhouselocation)
        // .config("hive.metastore.uris","thrift://localhost:9083")
        .config("hive.metastore.uris","thrift://innapindaiclm01.indiantdev.local:9083")
        .enableHiveSupport()
        .getOrCreate()*/
      
      /* To load file hdfs
       * val xmlStr = spark.sparkContext.addFile("hdfs:///user/s.d.arunachalam/self-closing_XML_3.xml")
       val xmlFileStr = SparkFiles.get("self-closing_XML_3.xml")
       val xmlFile = XML.load(xmlFileStr).toString();
       val result: Elem = scala.xml.XML.loadString(xmlFile)*/
       //spark.sqlContext.sql("CREATE TABLE IF NOT EXISTS sample.XMLtoKeyValue_1(key String, value STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'").show()
       val mapObject = linearize(result, result.label, Map[String, String]()).flatMap(_._3)
       
       mapObject.foreach {
       keyVal => 
         println(keyVal._1, keyVal._2)
       // spark.sqlContext.sql("insert into table sample.xmltoKeyValue4 values ('"+keyVal._1+"','"+keyVal._2+"')").show()
        }
         bw.close() 
 
                   
  }

}