package com.abarag4.hw2

import java.io.File

import com.abarag4.hw2.helpers.BinHelper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.chain.{ChainMapper, ChainReducer}
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

  val SLASH: String = "/"

  def main(args: Array[String]): Unit = {

    val startTime = System.nanoTime
    LOG.info("*** Starting MapReduceDriver ***")

    val verbose = configuration.getBoolean("configuration.verbose")
    val startTags = configuration.getString("configuration.startTags")
    val endTags = configuration.getString("configuration.endTags")

    val jobName0 = configuration.getString("configuration.jobName0")
    val jobName1 = configuration.getString("configuration.jobName1")
    val jobName2 = configuration.getString("configuration.jobName2")
    val jobName3 = configuration.getString("configuration.jobName3")
    val jobName4 = configuration.getString("configuration.jobName4")
    val jobName5 = configuration.getString("configuration.jobName5")
    val jobName6 = configuration.getString("configuration.jobName6")

    //Delete output_dir each time the map/reduce is run
    FileUtils.deleteDirectory(new File(outputFile));

    val conf: Configuration = new Configuration()

    //Set start and end tags for XmlInputFormat
    conf.set(MyXmlInputFormat.START_TAGS, startTags)
    conf.set(MyXmlInputFormat.END_TAGS, endTags)

    //Format as CSV output
    conf.set("mapred.textoutputformat.separator", ",")

    //Set Hadoop config parameters and start job
    val job0 = Job.getInstance(conf, jobName0)
    job0.setJarByClass(classOf[XMLTupleCheckMapper])
    job0.setMapperClass(classOf[XMLTupleCheckMapper])
    job0.setReducerClass(classOf[AuthorReducer])
    job0.setInputFormatClass(classOf[MyXmlInputFormat])
    job0.setOutputKeyClass(classOf[Text])
    job0.setOutputValueClass(classOf[IntWritable])
    job0.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job0, new Path(inputFile))
    FileOutputFormat.setOutputPath(job0, new Path((outputFile+SLASH+jobName0)))

    val job1 = Job.getInstance(conf, jobName1)
    job1.setJarByClass(classOf[NumberAuthorsMapper])
    job1.setMapperClass(classOf[NumberAuthorsMapper])
    job1.setReducerClass(classOf[AuthorReducer])
    job1.setInputFormatClass(classOf[MyXmlInputFormat])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job1, new Path(inputFile))
    FileOutputFormat.setOutputPath(job1, new Path((outputFile+SLASH+jobName1)))

    //Set Hadoop config parameters and start job
    val job2 = Job.getInstance(conf, jobName2)
    job2.setJarByClass(classOf[VenueMapper])
    job2.setMapperClass(classOf[VenueMapper])
    job2.setReducerClass(classOf[AuthorReducer])
    job2.setInputFormatClass(classOf[MyXmlInputFormat])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job2, new Path(inputFile))
    FileOutputFormat.setOutputPath(job2, new Path((outputFile+SLASH+jobName2)))

    //Set Hadoop config parameters and start job
    val job3 = Job.getInstance(conf, jobName3)
    job3.setJarByClass(classOf[AuthorScoreMapper])
    job3.setMapperClass(classOf[AuthorScoreMapper])
    job3.setReducerClass(classOf[AuthorScoreReducer])
    job3.setInputFormatClass(classOf[MyXmlInputFormat])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[DoubleWritable])
    job3.setOutputFormatClass(classOf[TextOutputFormat[Text, DoubleWritable]])
    FileInputFormat.addInputPath(job3, new Path(inputFile))
    FileOutputFormat.setOutputPath(job3, new Path((outputFile+SLASH+jobName3)))

    //Set Hadoop config parameters and start job
    val job4 = Job.getInstance(conf, jobName4)
    job4.setJarByClass(classOf[AuthorStatisticsMapper])
    job4.setMapperClass(classOf[AuthorStatisticsMapper])
    job4.setReducerClass(classOf[AuthorStatisticsReducer])
    job4.setInputFormatClass(classOf[MyXmlInputFormat])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[IntWritable])
    job4.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job4, new Path(inputFile))
    FileOutputFormat.setOutputPath(job4, new Path((outputFile+SLASH+jobName4)))

    //Set Hadoop config parameters and start job
    val job5 = Job.getInstance(conf, jobName5)
    job5.setJarByClass(classOf[SortingMapper])
    job5.setMapperClass(classOf[SortingMapper])
    job5.setReducerClass(classOf[SortingReducer])
    job5.setOutputKeyClass(classOf[DoubleWritable])
    job5.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job5, new Path(outputFile+SLASH+jobName3))
    FileOutputFormat.setOutputPath(job5, new Path((outputFile+SLASH+jobName5)))

    val job6 = Job.getInstance(conf, jobName6)
    job6.setJarByClass(classOf[AuthorVenueStatisticsMapper])
    job6.setMapperClass(classOf[AuthorVenueStatisticsMapper])
    job6.setReducerClass(classOf[AuthorStatisticsReducer])
    job6.setOutputKeyClass(classOf[Text])
    job6.setOutputValueClass(classOf[IntWritable])
    job6.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job6, new Path(inputFile))
    FileOutputFormat.setOutputPath(job6, new Path((outputFile+SLASH+jobName6)))

    LOG.info("*** Starting Job(s) NOW ***")
    if (job0.waitForCompletion(verbose) && job1.waitForCompletion(verbose) && job2.waitForCompletion(verbose) &&  job4.waitForCompletion(verbose) && job3.waitForCompletion(verbose) && job5.waitForCompletion(verbose) && job6.waitForCompletion(verbose)) {
    //if (job0.waitForCompletion(verbose)) {
      val endTime = System.nanoTime
      val totalTime = endTime - startTime
      LOG.info("*** FINISHED (Execution completed in: "+totalTime/1_000_000_000+" sec) ***")
    } else {
      LOG.info("*** FAILED ***")
    }
  }
  }
