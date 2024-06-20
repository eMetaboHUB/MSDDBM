package fr.inrae.brachemdb

import scopt.OptionParser

private case class Arguments(outputDir: String = "/data/braChemDb/",
                             command: String = "",
                             configFile: String = "msdQuery.conf")

object Main {
  def main(args: Array[String]): Unit = {

    val parser: OptionParser[Arguments] = new OptionParser[Arguments]("brachemdb") {

      opt[String]("outputDir")
        .abbr("od")
        .action((value, arguments) => arguments.copy(outputDir = value))
        .text("HDFS output dir, e.g. /data/braChemDb/ (default value)")
      note("\n")

      cmd("run")
        .text("run graph exploration and log output to [outputDir]/brachemdbOutput-[sys. time as millis]")
        .action((_, c) => c.copy(command = "run"))
      note("\n")


      cmd("makeDiff")
        .text("Compute difference between latest outputs, write results to [outputDir]/brachemdbResults")
        .action((_, c) => c.copy(command = "makeDiff"))
      note("\n")

      cmd("fakeRun1")
        .text("log fake output to [outputDir]/brachemdbOutput-[sys. time as millis] in order to test workflows")
        .action((_, c) => c.copy(command = "fakeRun1"))
      note("\n")

      cmd("fakeRun2")
        .text("log fake output with new results to [outputDir]/brachemdbOutput-[sys. time as millis] in order to test workflows")
        .action((_, c) => c.copy(command = "fakeRun2"))
      note("\n")

      cmd("test")
        .text("test taxonomy exploration. See results in /outputDir/brachemdbTest-<sysMillis>")
        .action((_, c) => c.copy(command = "test"))
      note("\n")

      opt[String]('c', "config")
        .valueName("config file location").action((value, arguments) => arguments.copy(configFile = value))
      note("\n")


    }

    def run(arguments: Arguments): Unit = {
      val e =  Explorer

      arguments.command match {
        case "run" =>
          e.buildSparkSession("BraChemDb_run")
          e.runExploration(arguments.outputDir,arguments.configFile,testContext = false,240,manualPartition = false)
        case "makeDiff" =>
          val f = Follower()
          f.makeDiff(arguments.outputDir,"brachemdbOutput-","brachemdbResults")

        case "fakeRun1" =>
          e.buildSparkSession("BraChemDb_fake_run1")
          e.fakeRun1(arguments.outputDir)
        case "fakeRun2" =>
          e.buildSparkSession("BraChemDb_fake_run2")
          e.fakeRun2(arguments.outputDir)
        case "test" =>
          e.buildSparkSession("BraChemDb_fake_self_test")
          e.selfTest(arguments.outputDir,arguments.configFile)

        case _ => println(parser.usage)

      }
    }

    parser.parse(args, Arguments()) match {
      case Some(arguments) => run(arguments)
      case None =>
    }

  }


}