version := "1.0"
scalaVersion :="2.11.12" // m3 ne va que jusqu'à 2.11
dockerBuildOptions += "--quiet"
dockerBaseImage := "openjdk:latest"

libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-model" % "4.2.3"
libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-storage" % "4.3.4"
libraryDependencies += "com.lihaoyi" %% "upickle" % "3.0.0-M2"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.9.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % "3.3.2"
libraryDependencies += "com.m3" %% "curly-scala" % "0.5.+"

enablePlugins(
  JavaAppPackaging,
  DockerPlugin
)

Docker / packageName := "metabohub/knapsack"
