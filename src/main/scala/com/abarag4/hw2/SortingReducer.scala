package com.abarag4.hw2

import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.javaapi.CollectionConverters.asScala

class SortingReducer extends Reducer[DoubleWritable,Text,Text,DoubleWritable] {

  val LOG: Logger = LoggerFactory.getLogger(getClass)

  private def handleTuple(key: DoubleWritable, value: Text, context:Reducer[DoubleWritable,Text, Text, DoubleWritable]#Context): Unit = {
    context.write(value, key)
  }

  override def reduce(key: DoubleWritable, values: java.lang.Iterable[Text], context:Reducer[DoubleWritable,Text, Text, DoubleWritable]#Context): Unit = {

    //LOG.debug("SortingReducer: "+key)

    //Now handle each tuple individually, flip key and value again
    values.forEach(v => handleTuple(key, v, context))
  }
}