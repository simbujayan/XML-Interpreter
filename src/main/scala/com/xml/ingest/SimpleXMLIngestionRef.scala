package com.xml.ingest

import scala.xml._
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
import scala.collection.mutable.MutableList

object SimpleXMLIngestionRef {
  var finalList = MutableList[String]()
   def linearize(node: Node, stack: String, map: Map[String, String], bw: BufferedWriter): List[(Node, String, Map[String, String])] = {
     (node, stack, map) :: node.child.flatMap {
      case e: Elem => {
          if (e.descendant.size == 1) {
             if(e.attributes.toList.size>=1)
              {           
            e.attributes.toList.foreach{f =>
            //println("Key:"+f.key+" Value:"+f.value)
            //println("Attrib:"+stack + ":" + e.label +":"+f.key->f.value)
            finalList += stack + ":" + e.label +":"+f.key
            var key = stack + ":" + e.label +":"+f.key
            var value = f.value
            bw.write(stack + ":" + e.label +":"+f.key+","+f.value.toString()+'\n')
             }
 
          }
               //println("The value of e.text is " +stack + ":" + e.label -> e.text)
               finalList += stack + ":" + e.label
               bw.write(stack + ":" + e.label+","+ e.text+'\n')
               linearize(e, stack, map ++ Map(stack + ":" + e.label -> e.text), bw)
          
        }
         else if(e.attributes!=null && e.attributes.toList.size>=1)
         {
            //println("Attrib:"+stack + ":" + e.label +":"+e.attributes+" = "+e.attributes.toList.size)
            var key = ""
            var value = ""
            e.attributes.toList.foreach
            {f =>
               finalList += stack + ":" + e.label +":" + f.key
               bw.write(stack + ":" + e.label +":" + f.key+","+f.value.toString()+'\n')
             }
             linearize(e, stack + ":" + e.label, map, bw)
         }
     
        else if(e.attributes.isEmpty && (e.child.size == 0) )
        {
           //println("self tag:"+stack + ":" + e.label  -> null)
            finalList += stack + ":" + e.label
            bw.write(stack + ":" + e.label+","+null+'\n')
            linearize(e, stack, map ++ Map(stack + ":" + e.label  -> null), bw)
        }
         
        else linearize(e, stack + ":" + e.label, map, bw)
      }        
      case _ => Nil
    }.toList

  }

  def main(args: Array[String]) {
     var xmlpath: String = (args(0)).trim()
     val output_filepath: String = (args(1)).trim()
     var file_split = output_filepath.lastIndexOf(".")
     var headStr = output_filepath.substring(0, file_split)
    // println(headStr)
     var tailStr = output_filepath.substring(file_split, output_filepath.length())
     //println(tailStr)
     var lookup = "_lookup"
     var reffilepath = headStr.concat(lookup).concat(tailStr)
     //println(reffilepath)
     val ref_filePath: String = headStr.concat(lookup).concat(tailStr).trim()
     //println("Ref file path:"+ref_filePath)
     val file = new File(output_filepath)
     val ref_file = new File(ref_filePath)
     val bw = new BufferedWriter(new FileWriter(file))
     val ref_writer = new BufferedWriter(new FileWriter(ref_file))
     //println("XML Path:"+xmlpath)
     //println("Output File Path:"+output_filepath)
     val xmlFile = XML.load(xmlpath).toString()
     val result: Elem = scala.xml.XML.loadString(xmlFile)
     val mapObject = linearize(result, result.label, Map[String, String](), bw).flatMap(_._3)
     var refList = finalList.distinct
     var ref_flag: Int = 1
         for(str <- refList)
         {
           var temp = str.split(":").last
           //println(str+","+temp+"_c"+ref_flag)
           ref_writer.write(str+","+temp+"_c"+ref_flag+'\n')         
           ref_flag +=1
         }
      bw.close() 
      ref_writer.close()
                     
  }
  
}