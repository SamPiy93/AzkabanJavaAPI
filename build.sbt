name := """azkaban"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayJava, PlayEnhancer)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  javaJdbc,
  cache,
  javaWs,
  filters,
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.apache.httpcomponents" % "httpcore" % "4.4.4",
  "org.apache.httpcomponents" % "httpmime" % "4.3.1",
  "com.google.code.gson" % "gson" % "2.3.1",
  "io.netty" % "netty-all" % "4.0.23.Final" force()
)
