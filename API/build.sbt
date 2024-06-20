
ThisBuild / scalaVersion := "2.12.16"
ThisBuild / version := "0.4.3"
ThisBuild / organization := "fr.inrae"
ThisBuild / organizationName := "inrae"

ThisBuild / coverageEnabled := false
ThisBuild / coverageExcludedFiles := ".*MsdRDFStatistics.*"

Compile / resourceGenerators += Def.task {
  val file = (Compile / resourceManaged).value / "msdDeployement" / "version"
  val contents = "VERSION=%s".format(version.value)
  IO.write(file, contents)
  Seq(file)
}.taskValue

//fork / run := true
//Test / fork := true
Test / parallelExecution := false //for Spark tests

javacOptions ++= Seq("-source", "11", "-target", "11")
Test / javacOptions ++= Seq("-source", "11", "-target", "11")
//WARNING : this version of Spark doesn't seem to support java version > 12, and the javacOption before is of no effect,
//as the problems comes from illegal JVM operations in Spark


assembly / test := {}


val sparkVersion = "3.2.1"



lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "sbtBuildInfo",
    name := "msddbm",

    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.2",
    ),

    libraryDependencies ++= Seq(
      // TODO:
      // Dependency maven:org.apache.hadoop:hadoop-common:3.3.3 is vulnerable, safe version 3.3.6 CVE-2022-25168 9.8 Improper Neutralization of Argument Delimiters in a Command ("Argument Injection") vulnerability with High severity
      // Dependency maven:commons-collections:commons-collections:20040616 is vulnerable Cx78f40514-81ff 7.5 Uncontrolled Recursion vulnerability with High severity
      // Dependency maven:org.apache.jena:jena-core:4.4.0 is vulnerable, safe version 4.9.0 CVE-2022-28890 9.8 Improper Restriction of XML External Entity Reference vulnerability with High severity
      // Dependency maven:org.apache.jena:jena-arq:4.4.0 is vulnerable, safe version 4.9.0 CVE-2023-32200 8.8 Improper Neutralization of Special Elements used in an Expression Language Statement ("Expression Language Injection") vulnerability with High severity

      ("org.apache.hadoop" % "hadoop-common" % "3.3.3")
        .exclude("org.apache.zookeeper", "zookeeper")
        .exclude("org.apache.avro", "avro-mapred")
        .exclude("com.fasterxml.jackson", "databind"),
      ("org.apache.hadoop" % "hadoop-client" % "3.3.3")
        .exclude("org.apache.zookeeper", "zookeeper")
        .exclude("org.apache.avro", "avro-mapred")
        .exclude("com.fasterxml.jackson", "databind"),
      "org.slf4j" % "slf4j-simple" % "2.0.9", // needed by hadoop
      "com.google.protobuf" % "protobuf-java" % "3.24.4", // needed by hadoop
      ("commons-collections" % "commons-collections" % "20040616")
        .exclude("org.apache.zookeeper", "zookeeper")
        .exclude("org.apache.avro", "avro-mapred")
        .exclude("com.fasterxml.jackson", "databind"), // needed by hadoop. Cannot update because of UnmodifiableMap pbm


      "com.lihaoyi" %% "upickle" % "3.1.4", //Json utils
      "com.github.scopt" %% "scopt" % "4.1.0", //command line parsing
      "com.typesafe" % "config" % "1.4.3", //reading config file
      //"org.apache.jena" % "apache-jena-libs" % "4.4.0",
      "org.apache.jena" % "jena-core" % "4.4.0",
      "org.apache.jena" % "jena-arq" % "4.4.0",

      "org.scalactic" %% "scalactic" % "3.2.17" % "test,provided", //tests
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test",
      ("net.sansa-stack" %% "sansa-rdf-spark" % "0.8.0-RC3")
        .exclude("org.apache.zookeeper", "zookeeper")
        .exclude("org.apache.avro", "avro-mapred")
        .exclude("com.fasterxml.jackson", "databind") % "test,provided",
      ("net.sansa-stack" %% "sansa-inference-spark" % "0.8.0-RC3")
        .exclude("org.apache.zookeeper", "zookeeper")
        .exclude("org.apache.avro", "avro-mapred")
        .exclude("com.fasterxml.jackson", "databind") % "test,provided",
      "org.scalatest" %% "scalatest" % "3.2.17" % "test,provided", //tests
      "org.apache.hadoop" % "hadoop-minicluster" % "3.3.3" % "test,provided", //tests
      "org.mockito" %% "mockito-scala" % "1.17.30" % "test,provided", //tests


    ),
    Compile / mainClass := Some("fr.inrae.msd.Main"),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    resolvers ++= Seq(
      "AKSW Maven Releases" at "https://maven.aksw.org/archiva/repository/internal",
      "AKSW Maven Snapshots" at "https://maven.aksw.org/archiva/repository/snapshots",
      "oss-sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Apache repository (snapshots)" at "https://repository.apache.org/content/repositories/snapshots/",
      "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/", "NetBeans" at "https://bits.netbeans.org/nexus/content/groups/netbeans/", "gephi" at "https://raw.github.com/gephi/gephi/mvn-thirdparty-repo/",
      Resolver.defaultLocal,
      Resolver.mavenLocal,
      "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
      "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
    ),
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    assembly / logLevel := Level.Info,
    Test / logLevel := Level.Debug,

    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { f =>
        f.data.getName.contains("sansa-stack")
      }
    },

    assembly / assemblyMergeStrategy := {

      case x if x.startsWith("javax") => MergeStrategy.first
      case x if x.startsWith("org/apache/commons/logging") => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x if x.contains("com.google") => MergeStrategy.first
      case x if x.contains("json-smart") => MergeStrategy.first
      case x if x.endsWith("msdQuery.conf") ||
        x.endsWith("msdQueryTest.conf") ||
        x.endsWith("msdQuery2.conf")
      => MergeStrategy.discard

      case x if Assembly.isConfigFile(x) =>
        MergeStrategy.concat
      case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
        MergeStrategy.rename
      case PathList("META-INF", xs @ _*) =>
        (xs map {_.toLowerCase}) match {
          case "services" :: xs =>
            MergeStrategy.filterDistinctLines
          case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
            MergeStrategy.discard
          case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
            MergeStrategy.discard
          case "plexus" :: xs =>
            MergeStrategy.discard

          case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
            MergeStrategy.filterDistinctLines
          case _ => MergeStrategy.deduplicate
        }
      case _ => MergeStrategy.deduplicate

    }
  )

Global / onChangedBuildSource := ReloadOnSourceChanges
