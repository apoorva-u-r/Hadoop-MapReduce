package com.abarag4.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

/**
 * This mapper is used in the following jobs:
 * - TupleChecker
 */
class XMLTupleCheckMapper extends Mapper[Object, Text, Text, IntWritable] {

  val one = new IntWritable(1)
  val bin = new Text

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /**
   *
   * This mapper just counts the total number of tuples and computes partial counts for each of the possible outer tags in the XML file.
   *
   * @param key Don't care
   * @param value XML data segment
   * @param context
   */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {

    val valueStr = value.toString;

    //Tuple count / outer XML tag
    bin.set(valueStr.substring(0, 5))
    context.write(bin, one)

    //Total tuple count
    bin.set("COUNT")
    context.write(bin, one)
  }

}
