package com.abarag4.hw2

import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

/**
 * This mapper is used for the following jobs:
 * - AuthorScoreOrdered
 */
class SortingMapper extends Mapper[Object, Text, DoubleWritable,Text] {

  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val keyText = new Text
  val valueWritable = new DoubleWritable()

  /**
   *
   * This mapper functions prepared the input CSV tuples for sorting.
   * The Key and Value positions are flipped so that the Hadoop framework performs the sorting automatically between the Map and Reduce phase.
   *
   * @param key Don't care
   * @param value CSV Tuple with format: key, value -> to be flipped to: value, key
   * @param context
   */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,DoubleWritable,Text]#Context): Unit = {
    //LOG.debug("SortingMapper: "+value)

    //Parse CSV here
    val inputTuple = value.toString.split(Constants.COMMA).map(_.trim)

    //Extract key
    keyText.set(inputTuple(0))

    //Extract value
    valueWritable.set(inputTuple(1).toDouble)

    //Flip
    context.write(valueWritable, keyText)
  }

}