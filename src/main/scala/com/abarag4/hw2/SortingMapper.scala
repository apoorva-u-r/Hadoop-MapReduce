package com.abarag4.hw2

import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

class SortingMapper extends Mapper[Object, Text, DoubleWritable,Text] {

  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val keyText = new Text
  val valueWritable = new DoubleWritable()

  override def map(key:Object, value:Text, context:Mapper[Object,Text,DoubleWritable,Text]#Context): Unit = {
    //LOG.debug("SortingMapper: "+value)

    //Parse CSV here
    val inputTuple = value.toString.split(Constants.COMMA).map(_.trim)

    keyText.set(inputTuple(0))
    valueWritable.set(inputTuple(1).toDouble)

    context.write(valueWritable, keyText)
  }

}