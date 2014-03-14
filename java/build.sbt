name := "datastore"

organization := "com.thefactory"

version := "1.0.7-SNAPSHOT"

scalaVersion := "2.10.2"

autoScalaLibrary := false

crossPaths := false

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.9" % "test",
  "io.netty" % "netty" % "3.6.6.Final",
  "junit" % "junit" % "4.11",
  "org.msgpack" % "msgpack" % "0.6.8",
  "org.xerial.snappy" % "snappy-java" % "1.0.5",
  "commons-logging" % "commons-logging" % "1.1.3",
  "org.scala-lang" % "scala-library" % "2.10.2"
)

publishMavenStyle := true

credentials += Credentials(Path.userHome / ".thefactory" / "credentials")

publishTo <<= version { (v: String) =>
  val nexus = "http://maven.thefactory.com/nexus/content/repositories/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "snapshots")
  else
    Some("releases" at nexus + "releases")
}
