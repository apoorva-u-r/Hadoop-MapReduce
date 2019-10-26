package com.abarag4.hw2

import com.abarag4.hw2.MapReduceDriver.getClass
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.javaapi.CollectionConverters.asScala

class AuthorStatisticsReducer extends Reducer[Text,IntWritable,Text,DoubleWritable] {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val value = new DoubleWritable()

  /**
   *
   * @param key Key for which tuples handled by this reducer are grouped.
   * @param values List of values of the tuples sent to this reducer with key "key".
   * @param context Output stream
   */
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, DoubleWritable]#Context): Unit = {

    val valuesList = scala.collection.mutable.ListBuffer.empty[Int]
    //values.forEach(u => LOG.debug("author: "+key+ " elem: "+u))
    values.forEach(u => valuesList.append(u.get())) //Append to List

    //Sorted ListBuffer
    val coauthors = valuesList.sorted

    val max = coauthors(coauthors.size-1) //max is last tuple since it's sorted
    val sum = coauthors.foldLeft(0.0) { (t,i) => t + i }
    val avg: Double = sum/coauthors.size.toDouble

    LOG.debug("author: "+key+" max: "+max+" sum: "+sum+" avg: "+avg)

    val tempKey = key.toString

    value.set(max.toDouble)
    key.set(tempKey+",max")
    //Write max tuple
    context.write(key, value)

    value.set(avg)
    key.set(tempKey+",avg")
    //Write avg tuple
    context.write(key, value)

    if (coauthors.length % 2 == 0) { //If array has even number of elements
      val median = (coauthors(coauthors.length/2).toDouble + coauthors((coauthors.length/2)-1).toDouble) / 2.0
      value.set(median)
    } else { //Else array has odd number of elements, so we just get the element in the middle
      val median = coauthors(coauthors.length/2).toDouble
      value.set(median)
    }
    //Write median tuple
    key.set(tempKey+",med")
    context.write(key, value)
  }
}
