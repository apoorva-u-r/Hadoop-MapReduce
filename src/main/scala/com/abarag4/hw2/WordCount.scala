package com.abarag4.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import scala.jdk.javaapi.CollectionConverters.asScala
import scala.jdk.javaapi.CollectionConverters.asJava

// This class performs the map operation, translating raw input into the key-value
// pairs we will feed into our reduce operation.
class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

  val one = new IntWritable(1)
  val word = new Text

  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context) = {
    for (t <-  value.toString().split("\\s")) {
      word.set(t)
      context.write(word, one)
    }
  }
}

// This class performs the reduce operation, iterating over the key-value pairs
// produced by our map operation to produce a result. In this case we just
// calculate a simple total for each word seen.
class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
    val sum = asScala(values).foldLeft(0) { (t,i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }
}

object WordCount {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val jobName = "JOB0"

  val inputFile = "input_dir"
  val outputFile = "output_dir"

  def main(args: Array[String]): Unit = {
    println("Hello, world!")

    val conf = new Configuration()

    val job = new Job(conf, jobName)
    job.setJarByClass(classOf[TokenizerMapper])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(inputFile))
    FileOutputFormat.setOutputPath(job, new Path((outputFile)))
    if (job.waitForCompletion(true)) {
      println("Map/Reduce finished")
    } else {
      println("Error")
    }
  }
}
