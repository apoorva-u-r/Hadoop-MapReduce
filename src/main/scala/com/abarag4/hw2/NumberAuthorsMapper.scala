package com.abarag4.hw2

import com.abarag4.hw2.MapReduceDriver.getClass
import com.abarag4.hw2.helpers.BinHelper
import com.typesafe.config.{Config, ConfigFactory}
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}
import java.io.File
import java.nio.file.Paths

import scala.xml.XML

/**
 * This is the Mapper used in the following jobs:
 * - NumberAuthors
 */
class NumberAuthorsMapper extends Mapper[Object, Text, Text, IntWritable] {

  val one = new IntWritable(1)
  val bin = new Text

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /**
   *
   * This Mapper function computes tuple with the following format (bin, num of publications)
   *
   * @param key Input key -> generic object, never used
   * @param value Input value -> RAW XML input segment, full tag block. e.g. <article> ... </article>
   * @param context Output stream
   */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {

    //Get dtd resource on filesystem
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    //This is used to correctly handle the parsing of tags and escaped entities in the XML file.
    //We encapsulate the xml input fragment into <dblp> tags and provide its dtd for parsing.
    //https://stackoverflow.com/questions/54851274/error-while-loading-the-string-as-xml-in-scala
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML
    val xml = scala.xml.XML.loadString(xmlComp)

    //Look for author tags
    val authors = (xml \\ "author")

    if (authors.nonEmpty) {
      bin.set(BinHelper.getBinFromCount(authors.size))

      //Write output tuple (e.g. ("2-3", 1))
      context.write(bin, one)
    }
  }
}
