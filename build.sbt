name := "Bisimulation"

version := "0.1"

retrieveManaged := true

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "com.lihaoyi" %% "upickle" % "0.7.4",
  "args4j" % "args4j" % "2.0.31" % "optional"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
