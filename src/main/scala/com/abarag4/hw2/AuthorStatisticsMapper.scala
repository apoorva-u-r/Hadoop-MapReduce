package com.abarag4.hw2

import com.abarag4.hw2.helpers.BinHelper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

class AuthorStatisticsMapper extends Mapper[Object, Text, Text, IntWritable] {
  val numCoauthors = new IntWritable(1)
  val authorKey = new Text

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  private def getNumOfCoAuthors(author: String, itemType: String, num: Int, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {

    authorKey.set(author+Constants.COMMA+itemType)
    numCoauthors.set(num) //or num-1?

    //Write output tuple (e.g. ("amedeo", 3))
    //LOG.debug("AuthorStatisticsMapper - OUTPUT: ("+author+", "+num+")")
    context.write(authorKey, numCoauthors)
  }

  /**
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
    val itemType = xml.child(0).label //"article"

    //Safety check, return immediately without adding tuples if no authors
    if (authors.isEmpty) {
      return
    }

    //For each author -> insert tuple
    authors.foreach(author => getNumOfCoAuthors(author.text.toString, itemType, authors.size, context))
  }
}
