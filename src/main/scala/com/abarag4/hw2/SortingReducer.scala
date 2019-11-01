package com.abarag4.hw2

import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.javaapi.CollectionConverters.asScala

/**
 * This reducer is used for the following jobs:
 * - AuthorScoreOrdered
 */
class SortingReducer extends Reducer[DoubleWritable,Text,Text,DoubleWritable] {

  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /**
   *
   * This helper function handles each tuple in the values iterable (there should be only one).
   * This is the function has actually performs the flip of key and value and writes the correct tuples to the output file.
   *
   * @param key Value of original tuple
   * @param value Key of original tuple
   * @param context
   */
  private def handleTuple(key: DoubleWritable, value: Text, context:Reducer[DoubleWritable,Text, Text, DoubleWritable]#Context): Unit = {
    context.write(value, key)
  }

  /**
   *
   * This Reducer flips back the Key and Value to their original position and writes the tuples to the output file.
   * At this time the tuples have been already sorted by the framework.
   *
   * @param key Value of original tuple
   * @param values Key of original tuple
   * @param context
   */
  override def reduce(key: DoubleWritable, values: java.lang.Iterable[Text], context:Reducer[DoubleWritable,Text, Text, DoubleWritable]#Context): Unit = {

    //LOG.debug("SortingReducer: "+key)

    //Now handle each tuple individually, flip key and value again
    values.forEach(v => handleTuple(key, v, context))
  }
}