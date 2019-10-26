#!/bin/bash

SCALA_VERSION="2.13"
JAR="amedeo_baragiola_hw2.jar"
VMIP="192.168.206.129"
MAIN="WordCount"

echo "Uploading .jar to VM.."
scp -P 2222 target/scala-$SCALA_VERSION/$JAR root@$VMIP:/root

echo "Starting Hadoop job.."
ssh root@$VMIP -p 2222 << EOF

#clean up
hdfs dfs -rm -r output_dir

#run hadoop
hadoop jar $JAR $MAIN input_dir output_dir

EOF
