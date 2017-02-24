name := "akka-stream-java8"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "1.1-M1+17316-0850fe32+20170227-1413"
)

fork in run := true
