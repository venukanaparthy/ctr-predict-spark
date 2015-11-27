name := "CTRPredict"

version := "0.1"

scalaVersion := "2.10.4"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

resourceDirectory in Compile := baseDirectory.value / "resources"

mainClass in (Compile, run) := Some("CTRPredict")

