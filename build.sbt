name := "akka-stream-java8"

version := "1.0"

scalaVersion := "2.11.8" 

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.6"
)

fork in run := true
