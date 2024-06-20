package fr.inrae.msd

import org.apache.spark.sql.SparkSession
import scopt.{OptionDef, OptionParser}
import sbtBuildInfo.BuildInfo


object Main {


  private case class Arguments(configFile: String = "msdQuery.conf",
                               command: String = "",
                               graphName: String = "",
                               graphVersion: String = "",
                               outputDir: String = "",
                               list: Seq[String] = Vector(""),
                               sparkStop: Boolean = true,
                               parallelism: Int = 48,
                               maxOntologySize: Long = 5000,
                               useHorstReasoner: Boolean = false,
                               maxTriplesPerFile: Long = 2000000
                              )

  val apiVersion: String = BuildInfo.version


  def main(args: Array[String]): Unit = {


    val parser: OptionParser[Arguments] = new OptionParser[Arguments](BuildInfo.name) {


      def graphNameOpt: OptionDef[String, Arguments] =
        opt[String]("name")
          .abbr("n")
          .action((value, arguments) => arguments.copy(graphName = value))
          .text("graph name")
          .required()

      def graphVersionOpt: OptionDef[String, Arguments] =
        opt[String]("version")
          .abbr("v")
          .action((value, arguments) => arguments.copy(graphVersion = value))
          .text("graph version")

      def outputDirOption: OptionDef[String, Arguments] =
        opt[String]("outputDir")
          .abbr("od")
          .action((value, arguments) => arguments.copy(outputDir = value))
          .text("output directory")

      def sparkStopOption: OptionDef[Boolean, Arguments] =
        opt[Boolean]("sparkStop")
          .action((value, arguments) => arguments.copy(sparkStop = value))
          .text("Stop spark after job - default = true, option for tests with multiple jobs")
          .hidden()


      opt[String]('c', "config")
        .valueName("config file location").action((value, arguments) => arguments.copy(configFile = value))
      note("\n")

      cmd("help")
        .text("Display usage")
        .action((_, c) => c.copy(command = "help"))
      note("\n")

      cmd("version")
        .text("Get API version")
        .action((_, c) => c.copy(command = "version"))
      note("\n")

      cmd("listAllGraphs")
        .text("List available graphs on HDFS, all versions")
        .action((_, c) => c.copy(command = "listAllGraphs"))
      note("\n")

      cmd("listGraphs")
        .text("List available graphs on HDFS, last version only")
        .action((_, c) => c.copy(command = "listGraphs"))
      note("\n")

      cmd("gs")
        .text("List available graphs with their sizes, if available in their VoID analysis. Slower than listGraphs")
        .action((_, c) => c.copy(command = "gs"))
      note("\n")

      cmd("lsi")
        .text("List instance graphs only")
        .action((_, c) => c.copy(command = "lsi"))
      note("\n")

      cmd("lso")
        .text("List ontologies only")
        .action((_, c) => c.copy(command = "lso"))
      note("\n")

      cmd("metadata")
        .text("Print basic metadata written during ingestion for a given graph (last version if not specified with -gv)")
        .action((_, c) => c.copy(command = "metadata"))
        .children(graphNameOpt, graphVersionOpt)
      note("\n")

      cmd("stats")
        .text("List void stats of a given graph (last version if not specified with -gv)")
        .action((_, c) => c.copy(command = "stats"))
        .children(graphNameOpt, graphVersionOpt)
      note("\n")

      cmd("findProp")
        .text("List graphs having given properties")
        .action((_, c) => c.copy(command = "findProp"))
        .children(
          opt[Seq[String]]("list")
            .abbr("l")
            .valueName("<p1>,<p2>...")
            .action((value, arguments) => arguments.copy(list = value))
            .text("List of properties")
            .required(),
        )
      note("\n")

      cmd("findClass")
        .text("List graphs having given classes")
        .action((_, c) => c.copy(command = "findClass"))
        .children(
          opt[Seq[String]]("list")
            .abbr("l")
            .valueName("<c1>,<c2>...")
            .action((value, arguments) => arguments.copy(list = value))
            .text("List of classes")
            .required(),
        )
      note("\n")

      cmd("listFilesUris")
        .text("List files URIs of a given graph (last version if not specified with -gv)")
        .action((_, c) => c.copy(command = "listFilesUris"))
        .children(graphNameOpt, graphVersionOpt)
      note("\n")

      cmd("listFilesPaths")
        .text("List HDFS paths of files of a given graph, last version if not specified with -gv")
        .action((_, c) => c.copy(command = "listFilesPaths"))
        .children(graphNameOpt, graphVersionOpt)
      note("\n")

      cmd("listFilesNames")
        .text("List names of the files of a given graph, last version if not specified with -gv")
        .action((_, c) => c.copy(command = "listFilesNames"))
        .children(graphNameOpt, graphVersionOpt)
      note("\n")

      cmd("void")
        .text("Extract graph void statistics - launch with park-submit")
        .action((_, c) => c.copy(command = "void"))
        .children(
          graphNameOpt,
          graphVersionOpt,
          outputDirOption,
          sparkStopOption,
        )
      note("\n")

      cmd("enhanceVoid")
        .text("Enhance VoID statistics with property ranges and domains - launch with park-submit")
        .action((_, c) => c.copy(command = "enhanceVoid"))
        .children(
          graphNameOpt,
          graphVersionOpt,
          outputDirOption,
          sparkStopOption,
          opt[Int]("parallelism")
            .abbr("p")
            .action((value, arguments) => arguments.copy(parallelism = value))
            .text("Parallelism. Default = 48"),

          opt[Boolean]("useHorstReasoner")
            .abbr("uhr")
            .action((value, arguments) => arguments.copy(useHorstReasoner = value))
            .text("Use Horst OWL reasoner instead of simple RDFS. Default = false"),

          opt[Long]("maxOntologySize")
            .abbr("mos")
            .action((value, arguments) => arguments.copy(maxOntologySize = value))
            .text("Ontology size limit (in triples) to be used for inference")


        )
      note("\n")
      cmd("toParquet")
        .text("save graph as parquet")
        .action((_, c) => c.copy(command = "toParquet"))
        .children(
          graphNameOpt,
          graphVersionOpt,
          outputDirOption,
          sparkStopOption,
          opt[Long]("maxTriplesPerFile")
            .abbr("mtpf")
            .action((value, arguments) => arguments.copy(maxTriplesPerFile = value))
            .text("Maximal number of triples per parquet file. Default is 2,000,000")
        )
      note("\n")


    }


    def run(arguments: Arguments): Unit = {


      if (!new java.io.File(arguments.configFile).exists()) {
        println(s"Config file ${arguments.configFile} doesn't exist. Stopping")
        return
      }
      val interactiveFunctions = new InteractiveFunctions(arguments.configFile)


      arguments.command match {
        case "listAllGraphs" => interactiveFunctions.listAllGraphs()
        case "listGraphs" => interactiveFunctions.listAllGraphs(false)
        case "metadata" => interactiveFunctions.metadata(arguments.graphName, arguments.graphVersion)
        case "listFilesUris" => interactiveFunctions.listFilesUris(arguments.graphName, arguments.graphVersion)
        case "listFilesPaths" => interactiveFunctions.listFilesPaths(arguments.graphName, arguments.graphVersion)
        case "listFilesNames" => interactiveFunctions.listFilesNames(arguments.graphName, arguments.graphVersion)
        case "stats" => interactiveFunctions.graphStats(arguments.graphName, arguments.graphVersion)
        case "findProp" => interactiveFunctions.findAndPrintSchemaItems(arguments.list.map(PropertyItem))
        case "findClass" => interactiveFunctions.findAndPrintSchemaItems(arguments.list.map(ClassItem))
        case "lsi" => interactiveFunctions.printInstanceGraphs()
        case "lso" => interactiveFunctions.printOntologies()
        case "gs" => interactiveFunctions.printGraphSizes()


        //Spark Jobs
        //case "sparqlRequest" => sparkFunctions.runSparkJob(sparkFunctions.sparqlRequest, Map("version" -> arguments.graphVersion, "name" -> arguments.graphName))
        case "void" =>
          val spark = getSparkSession("void")
          val sparkTools = MsdSparkTools(arguments.configFile)
          sparkTools.void(arguments.graphName, arguments.graphVersion, arguments.outputDir)
          if(arguments.sparkStop) spark.stop()
        case "enhanceVoid" =>
          val spark = getSparkSession("enhanceVoid")
          val sparkTools = MsdSparkTools(arguments.configFile)
          sparkTools.enhanceVoid(arguments.graphName, arguments.graphVersion, arguments.outputDir, arguments.parallelism, arguments.maxOntologySize, arguments.useHorstReasoner)
          if(arguments.sparkStop) spark.stop()
        case "toParquet" =>
          val spark = getSparkSession("toParquet")
          val sparkTools = MsdSparkTools(arguments.configFile)
          sparkTools.serializeGraphToParquet(arguments.graphName, arguments.graphVersion, arguments.maxTriplesPerFile)
          if(arguments.sparkStop) spark.stop()
        case "version" => println(s"Version : $apiVersion, with Scala version ${BuildInfo.scalaVersion}")
        case _ => println(parser.usage)
      }


    }


    parser.parse(args, Arguments()) match {
      case Some(arguments) => run(arguments)
      case None =>
    }

  }

  private def getSparkSession(functionName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(s"msdlib-${functionName}")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
      .getOrCreate()

  }
}

