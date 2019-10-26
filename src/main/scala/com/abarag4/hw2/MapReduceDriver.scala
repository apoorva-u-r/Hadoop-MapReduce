package com.abarag4.hw2

import java.io.File

import com.abarag4.hw2.helpers.BinHelper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.slf4j.{Logger, LoggerFactory}

/*
 * This class contains the Map/Reduce driver for task 1.
 */
object MapReduceDriver {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  //Read input and output file paths from configuration file
  val inputFile: String = configuration.getString("configuration.inputFile")
  val outputFile: String = configuration.getString("configuration.outputFile")

  def main(args: Array[String]): Unit = {
    println("*** NumberAuthorsDriver ***")

    val verbose = configuration.getBoolean("configuration.verbose")
    val startTags = configuration.getString("configuration.startTags")
    val endTags = configuration.getString("configuration.endTags")

    val jobName1 = configuration.getString("configuration.jobName1")
    val jobName2 = configuration.getString("configuration.jobName2")
    val jobName3 = configuration.getString("configuration.jobName3")
    val jobName4 = configuration.getString("configuration.jobName4")

    //Delete output_dir each time the map/reduce is run
    FileUtils.deleteDirectory(new File(outputFile+"1"));
    FileUtils.deleteDirectory(new File(outputFile+"2"));
    FileUtils.deleteDirectory(new File(outputFile+"3"));
    FileUtils.deleteDirectory(new File(outputFile+"4"));
    FileUtils.deleteDirectory(new File(outputFile+"5"));

    val conf = new Configuration()

    //Set start and end tags for XmlInputFormat
    conf.set(XmlInputFormat.START_TAGS, startTags)
    conf.set(XmlInputFormat.END_TAGS, endTags)

    //Format as CSV output
    conf.set("mapred.textoutputformat.separator", ",")

    //Set Hadoop config parameters and start job

    val job1 = Job.getInstance(conf, jobName1)
    job1.setJarByClass(classOf[NumberAuthorsMapper])
    job1.setMapperClass(classOf[NumberAuthorsMapper])
    job1.setReducerClass(classOf[AuthorReducer])
    job1.setInputFormatClass(classOf[XmlInputFormat])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job1, new Path(inputFile))
    FileOutputFormat.setOutputPath(job1, new Path((outputFile+"1")))

    //Set Hadoop config parameters and start job
    val job2 = Job.getInstance(conf, jobName2)
    job2.setJarByClass(classOf[VenueMapper])
    job2.setMapperClass(classOf[VenueMapper])
    job2.setReducerClass(classOf[AuthorReducer])
    job2.setInputFormatClass(classOf[XmlInputFormat])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job2, new Path(inputFile))
    FileOutputFormat.setOutputPath(job2, new Path((outputFile+"2")))

    //Set Hadoop config parameters and start job
    val job3 = Job.getInstance(conf, jobName3)
    job3.setJarByClass(classOf[AuthorScoreMapper])
    job3.setMapperClass(classOf[AuthorScoreMapper])
    job3.setReducerClass(classOf[AuthorScoreReducer])
    job3.setInputFormatClass(classOf[XmlInputFormat])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[DoubleWritable])
    job3.setOutputFormatClass(classOf[TextOutputFormat[Text, DoubleWritable]])
    FileInputFormat.addInputPath(job3, new Path(inputFile))
    FileOutputFormat.setOutputPath(job3, new Path((outputFile+"3")))

    //Set Hadoop config parameters and start job
    val job4 = Job.getInstance(conf, jobName4)
    job4.setJarByClass(classOf[AuthorStatisticsMapper])
    job4.setMapperClass(classOf[AuthorStatisticsMapper])
    job4.setReducerClass(classOf[AuthorStatisticsReducer])
    job4.setInputFormatClass(classOf[XmlInputFormat])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[IntWritable])
    job4.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job4, new Path(inputFile))
    FileOutputFormat.setOutputPath(job4, new Path((outputFile+"4")))

    //Set Hadoop config parameters and start job
    val job5 = Job.getInstance(conf, jobName4)
    job5.setJarByClass(classOf[SortingMapper])
    job5.setMapperClass(classOf[SortingMapper])
    job5.setReducerClass(classOf[SortingReducer])
    job5.setOutputKeyClass(classOf[DoubleWritable])
    job5.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job5, new Path("output_dir3_clean"))
    FileOutputFormat.setOutputPath(job5, new Path((outputFile+"5")))

    //if (job2.waitForCompletion(verbose) && job1.waitForCompletion(verbose) && job3.waitForCompletion(verbose) && job4.waitForCompletion(verbose)) {
    //if (job3.waitForCompletion(verbose) && job5.waitForCompletion(verbose)) {
    if (job5.waitForCompletion(verbose)) {
    println("Map/Reduce finished")
    } else {
      println("Error")
    }
  }
  }
