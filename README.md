# Amedeo Baragiola - HW2 CS441
#### Politecnico di Milano
#### University of Illinois at Chicago

## Introduction

This homework consists in creating different map/reduce jobs in order to produce multiple statistics for a publication dataset: https://dblp.uni-trier.de.
The hadoop framework has been extensively used as part of this homework.

## Requirements

* HDP Sandbox (https://www.cloudera.com/downloads/hortonworks-sandbox.html) imported in VMWare. Please read the documentation present on the cloudera.com website for instruction on how to set up and configure the Sandbox.
* A copy of the dblp.xml file is available in the input_dir folder. (This folder needs to be created in the root folder of the project)
* ssh and scp installed and working on the host system.

### Test setup

* HDP Sandbox
* MacOS High Sierra
* VMware Fusion 11.5
* dblp.xml (md5: 2fe9e5daab5350d60323fd35b220eb5b)

## Installation instructions
This section contains the instructions on how to run the simulations implemented as part of this homework, the recommended procedure is to use IntellJ IDEA with the Scala plugin installed.

1. Open IntellJ IDEA, a welcome screen will be shown, select "Check out from Version Control" and then "Git".
2. Enter the following URL and click "Clone": https://bitbucket.org/abarag4/amedeo_baragiola_hw2.git
3. When prompted confirm with "Yes"
4. The SBT import screen will appear, proceed with the default options and confirm with "OK"
5. Confirm overwriting with "Yes"
6. You may now go to src/main/scala/com.abarag4/ where you can find the main rap/reduce driver class: MapReduceDriver. A run configuration is automatically created when you click the green arrow next to the main method of the driver class.

Note: All the required dependencies have been included in the build.sbt and plugins.sbt files. In case dependencies can't be found, check that the maven URLs are still current.
Note (input data): The input .xml file needs to be copied in the input_dir folder, otherwise the job execution will fail.

#### Alternative: SBT from CLI

If you don't want to use an IDE, you may run this project from the command line (CLI), proceed as follows:

1. Type: git clone https://bitbucket.org/abarag4/amedeo_baragiola_hw2.git
2. Before running the actual code, you may wish to run tests with "sbt clean compile test"
3. Run the code: "sbt clean compile run"

Note: "sbt clean compile run" performs a local execution of the map/reduce jobs. The jobs are run in sequence, you may wish to read the next section on how to assemble an actual jar.
Note (input data): The input .xml file needs to be copied in the input_dir folder, otherwise the job execution will fail.

#### Creating a JAR file

You may wish to create a Jar file so you can run the map/reduce jobs on AWS EMR or on a virtual machine that provides an Hadoop installation, proceed as follows:

1. Type: git clone https://bitbucket.org/abarag4/amedeo_baragiola_hw2.git
2. Run the following command: "sbt clean compile assembly"
3. A Jar will be created in the target/scala-2.13/ folder with the following name: amedeo\_baragiola\_hw2.jar.

Note: Make sure that the Java version present on the machine you compile the code on is older or matches the version on the machine you run it on. Java JDK 1.8 is recommended.

## Deployment instructions (HDP Sandbox)

You may wish to deploy this homework on the HDP Sandbox in order to run it and test its functionality. Start by creating a Jar file as outlined in the previous section, then proceed as follows:

1. Make sure the HDP Sandbox is running and it has fully started. This make take a while even on a powerful machine. You may check by logging in the web panel and checking the status on the "Start services" task.
2. Copy the jar file to the Sandbox by issuing the following command: "scp -P 2222 target/scala-2.13/amedeo\_baragiola\_hw2.jar root@sandbox-hdp.hortonworks.com:/root"
3. Copy the dblp.xml to the Sandbox: "scp -P 2222 dblp.xml root@sandbox-hdp.hortonworks.com:/root"
4. Login into the Sandbox: ssh -p 2222 -l root sandbox-hdp.hortonworks.com. You may be asked for a password if you have not set up SSH keys. The default password is: hadoop
5. Create the input directory on HDFS: hdfs dfs -mkdir input_dir
6. Load the dataset on HDFS: hdfs dfs -put dblp.xml input_dir/
7. You can now launch the job: hadoop jar amedeo_baragiola_hw2.jar
8. After completion the results are saved in a folder named output_dir on HDFS, you can copy them to local storage by issuing the following command: "hdfs dfs -get output_dir output"
9. Exit from the SSH terminal, "exit"
10. Copy the results to the host machine: scp -P 2222 -r root@sandbox-hdp.hortonworks.com:/root/output <local_path>

Note: Instead of just copying the data from HDFS, I recommend merging the output files.
In order to do that, issue the following command: "hadoop fs -getmerge /output_dir/job_specific_dir/ <local_path>.
You may list job specific output dirs by issuing: "hdfs dfs -ls output_dir".

Additional note: Depending on your DNS settings on the your host machine the hostname sandbox-hdp.hortonworks.com may fail to resolve, if this happens you can either add it to /etc/hosts or use the IP address of the Sandbox instead in the commands above.

## Map/Reduce jobs

As part of this homework a total of 6 map/reduce jobs were created.
Those are listed below by job name, the relevant classes have intuitive matching names. The association between the classes involved and each job is clearly listed in the MapReduceDriver class.

* XMLTupleChecker: This job checks that the parsing of the XML file is done corrently by the XMLInputFormat class. See the specific section for more details.
* NumberAuthors: This job bins the publications by number of authors. The output tuples have the following format: (bin, number of pub). (e.g. (2-3, 10))
* AuthorVenue: This job bins the publications by number of authors, venue (article, phdthesis, etc..) and year of publication. Tuples have a composite key, while their value is the number of publications that fit into that category.
* AuthorScore: This job maps each author to their authorship score. The output tuples have the following format: (AuthorName, AuthorshipScore)
* AuthorScoreOrdered: This job sorts the authors by authorship score using the map/reduce framework. It depends on the previous job and the output tuples are the same, but sorted.
* AuthorStatistics: This job produces the max, avg and median of the number of co-authors for each author in the dataset.
* AuthorVenueStatistics: This job produces the max, avg and median of the number of co-authors for each author and each publication venue (article, phdthesis, etc..) in the dataset.

### File splitting

Firstly, the input XML file is split in data chunks, this is done on a filesystem level by HDFS with a default chunk size of 128 MB.
On a logical level the Hadoop framework provides the InputSplit concept, however, the MyXMLInputFormat custom class (Apache Mahout is used as a starting point, but extensive modifications have been made) embeds the logic to correctly split XML files.

The default TextInputFormat provided by the framework splits files line-by-line, this is not acceptable for XML as the minimum logical unit of an XML file is a tag.
The custom made MyXMLInputFormat class enforces exactly that by producing splits so that XML tags are never cut in different parts.

The way the MyXMLInputFormat works is by allowing splits as little as a single XML outer tag (e.g. article, phdthesis, etc..).

It is important to point out, however, that this doesn't translate in a number of mappers equal to the number of XML outer tags present in the file.
This is because the number of mappers started, by default, actually depends on how the data is split on the filesystem level. (e.g. 2.8 GB is about 21 mappers: 2800/128 = 21)

### Input/Output Formats

An important part of computing the required statistics through the map/reduce framework is defining the input/output formats for each of the map/reduce jobs one may wish to implement.

The input of each mapper is a chunk of XML data containing XML outer tags. These XML tags have been obtained as described above through the use of the MyXMLInputFormat class.
In particular we have input tuples as: (Object, Text). The key is a don't care here, while the Text contains XML data.

The output tuples differ for the various jobs, in particular some jobs have joint keys (concat with , ) while others have simpler ones.
The values depend of the specific task performed. Output tuples are as follows:

* NumberAuthors -> Output tuple: (bin, num of publications) e.g. (2-3, 2516097).
* AuthorVenue -> Output tuple: ((bin, venue, year), num of publications) e.g. ((1,article,1991-2000), 74737).
* AuthorScore -> Output tuple: (author name, authorship score) e.g. (A Kitaygorodksy, 1.05).
* AuthorScoreOrdered -> Input tuple: (author name, authorship score)  Output tuple: (author name, authorship score) but sorted.
* AuthorStatistics -> Output tuple: ((author name, max/avg/med), statistic value) e.g. ((A Kitaygorodksy,max), 18.0), ((A Kitaygorodksy,avg), 9.5), ((A Kitaygorodksy,med), 9.5).
* AuthorVenueStatistics -> Output tuple: ((author name, venue, max/avg/med), statistic value) e.g. ((A Clara Kanmani,article,max), 3.0), ((A Clara Kanmani,article,avg), 3.0), ((A Clara Kanmani,article,med), 3.0).

## Where to locate the results in the repository

In this section the location of the results for each of the assigned tasks are given, so that the reader can easily find them in the repo. The .txt files are in csv format.

* NumberAuthors: charts/number_authors.png, csv/results.zip -> number_authors.txt
* AuthorVenue: charts/authorvenue1.png, charts/authorvenue2.png, csv/results.zip -> author_venue.txt
* AuthorScore: csv/top_100.txt, csv/bottom_100.txt, csv/results.zip -> author_score_tuples.txt
* AuthorScoreOrdered: csv/results.zip -> author_score_ordered.txt
* AuthorStatistics: csv/results.zip -> author_statistics.txt
* AuthorVenueStatistics: csv/results.zip -> author_venue_statistics.txt

## Creating charts

Upon completion of the map/reduce jobs the output is produced in csv format. The different jobs put output data in job specific directories.
If you have previously merged the output files (recommended) you shall now have one output file for each job. You are required to name the files appropriately.

From now on, we assume that the files will be named as job_specific_dir.txt, for example, for the first job listed above the job specific directory is: number_authors, so the output, when merged, should be called: number_authors.txt
If different names are used changes will need to be made in the relevant files.

### Requirements to plot charts

* Python notebooks (Jupyter or JupyterLab installed. Installation instructions here: https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html
* Pandas library for Python notebooks. Instructions here: https://pandas.pydata.org/pandas-docs/stable/install.html
* PyPlot for Python notebooks. Instructions here: https://matplotlib.org/3.1.1/users/installing.html

In order to create charts an industry-standard tool in the data science world has been used: Pandas.
A Python notebook with the relevant code can be found in the "Graphs.ipynb" file in the repository charts/ folder, the following instructions explain how to run it:

1. Make sure that the requirements listed above have been installed.
2. Open Jupyter or JupyterLab and drag the Graphs.ipynb to open it.
3. Copy the output files from the map/reduce jobs in the same folder as the .ipynb file.
4. Click on "Run" -> "Run all cells".

The charts/ folder in the repository root also contains the charts obtained by following the procedure above.

Note: Make sure that the file names match those listed in the python notebook source code, if you use different names you may need to change them.
Note of stacked charts: Stacked charts are created by stacking on top of each other the columns by venue or year.
This means that to obtain the total number for a certain bin one needs to sum each color in the bar starting from 0.
In other words the bar shown for each color does not represent a percentage, but an asbolute value instead.

## Sorting algorithm using map/reduce & Top/Bottom 100 lists

The sorting algorithm is implemented through the map/reduce framework, this provides high-level parallelism and relies on the framework to perform the sorting operation, instead of doing so manually.
A map/reduce job is dedicated to perform sorting of the tuples produced by the AuthorScore job; it resolves around the key observation that the Hadoop framework sorts the keys of the tuples in descending order by default during the shuffling operation (between Map and Reduce).
Therefore, in the Mapper function we flip the tuple key with its value and viceversa. e.g. (Key, Value) -> (Value, Key); while in the Reducer we perform the flip again, restoring the correct placement of keys and values.

This allows us to obtain a list of sorted (Author, score) tuples; furthermore by imposing a restriction on the number of reducers (number of reducers = 1) we are able to obtain a single ordered list in csv format. (instead of multiple part* files)

#### Top 100/Bottom 100 lists

We can now rely on the widely available Unix tools "head" and "tail" to cut the output csv and create a list of only the first 100 and last 100 authors.
In order to do this, proceed as follows:

1. Open the terminal app and go to the directory that contains the output file
2. Run the following command: "head -n 100 output_file > bottom_100.txt"
3. Run the following command: "tail -n 100 output_file > top_100.txt"

The resulting lists of the top and bottom 100 authors by authorship score have been saved in the files: "bottom_100.txt" and "top_100.txt".

## AWS EMR Deployment

A YouTube video showing the AWS EMR deployment process is available here: https://www.youtube.com/watch?v=NwX04rRdOdo

## Verify the parsing results

A big chunk of this homework is writing a working XML parser, such utility class is not present in the Hadoop framework by default;
An implementation by Apache Mahout is present at the following URL: https://github.com/apache/mahout/blob/758cfada62556d679c445416dff9d9fb2a3c4e59/community/mahout-mr/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java

However, this implementation does not support multiple starting tags and it therefore requires extensive changes; this is why the problem of verifying the correctness of such implementation becomes relevant.
In order to tackle such issues a specific map/reduce job "TupleChecker" has been created, this job has the purpose of producting partial counts of all the top tags present in the XML file (such as: article, phdthesis, www, etc..) so that they can be compared with the results obtained from "grep" on Unix.

The following procedure briefly outlines how to do so:

1. Run the TupleChecker map/reduce job on the entire dataset or on a subset of it.
2. Open a terminal and move to the folder in which the dblp.xml file is stored.
3. Issue grep commands such as: "grep -ri '<article' dblp.xml | wc -l" for each of the tags returned by the TupleChecker job.
4. Confirm that the results match.
