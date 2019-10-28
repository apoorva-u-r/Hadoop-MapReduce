package com.abarag4.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

class XMLTupleCheckMapper extends Mapper[Object, Text, Text, IntWritable] {

  val one = new IntWritable(1)
  val bin = new Text

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {

    val valueStr = value.toString;

    bin.set(valueStr.substring(0, 10))
    context.write(bin, one)

    bin.set("COUNT")
    context.write(bin, one)
  }

}
