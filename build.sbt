name := "sparkie"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.4.1"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4" % "test"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2"
