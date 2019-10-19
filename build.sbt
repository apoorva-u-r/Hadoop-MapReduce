name := "Amedeo_Baragiola_hw2"

version := "0.1"

scalaVersion := "2.13.1"

//https://stackoverflow.com/questions/25144484/sbt-assembly-deduplication-found-error
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.4"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

mainClass in (Compile, run) := Some("com.abarag4.hw2.WordCount")
mainClass in assembly := Some("com.abarag4.hw2.WordCount")

assemblyJarName in assembly := "amedeo_baragiola_hw2.jar"

