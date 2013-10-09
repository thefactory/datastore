name := "datastore"

organization := "com.thefactory"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.9" % "test",
  "io.netty" % "netty" % "3.6.6.Final",
  "junit" % "junit" % "4.11",
  "org.msgpack" % "msgpack" % "0.6.8",
  "org.xerial.snappy" % "snappy-java" % "1.0.4.1"
)
