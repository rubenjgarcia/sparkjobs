name := "SparkJobs"

version := "0.1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"