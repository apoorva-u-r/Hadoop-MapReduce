package com.abarag4.hw2

import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.jdk.javaapi.CollectionConverters.asScala

class AuthorScoreReducer extends Reducer[Text,DoubleWritable,Text,DoubleWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[DoubleWritable], context:Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {

    val initial: Double = 0.0
    val sum = asScala(values).foldLeft(initial) {
      (t,i) => t + i.get()
    }

    //Write output tuple
    context.write(key, new DoubleWritable(sum))
  }
}
