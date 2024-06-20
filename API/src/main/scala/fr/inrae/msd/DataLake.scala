package fr.inrae.msd

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import sbtBuildInfo.BuildInfo

import java.io.File
import scala.Console.err
import scala.annotation.tailrec


/** Represents a datalake containing RDF graphs previously ingested with the MSDDBM Airflow setup
 *
 * @param fs     Datalake Hadoop filesystem where graps are located
 * @param config Config file path for the datalake, specifying mainly hadoop.DATA_DIR as directory holding the graphs and hadoop.LOG_FILE as general purpose log file. See resources/msdQuery.conf as example file.
 *
 */
class DataLake(config: String, commandLineContext: Boolean = false) {

  lazy val fileSystem: FileSystem = if (commandLineContext)
    HadoopConfig(config).fileSystem
  else
    FileSystem.get(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)


  val configFile: String = config
  val applicationConf: Config = ConfigFactory.parseFile(new File(configFile))
  val dataDir: String = applicationConf.getString("datalake.DATA_DIR")
  private val logFileName: String = applicationConf.getString("datalake.LOG_FILE")
  private val dataPath: Path = new org.apache.hadoop.fs.Path(dataDir)


  val apiVersion: String = BuildInfo.version

  private val analysisDirNameSuffix = "_analysis/" // ex. /data/dublin_core_dcmi_terms_v2020-01-20_analysis/
  private val voidDirName = "void" // ex. /data/dublin_core_dcmi_terms_v2020-01-20_analysis/void


  /** Returns the specified graph, if found in the datalake
   *
   * @param name    graph name as specified in the dir_name Airflow DAG parameter
   * @param version optional version value
   * @return corresponding KnowledgeGraph object if found, otherwise throws a NoSuchElementException
   */
  def getGraph(name: String, version: String = ""): KnowledgeGraph = {

    val foundGraph = if (version == "")
      Try(getLatestGraphs.filter(kg => kg.dirname.equals(name)).head)
    else {
      Try(getAllGraphs.filter(kg => kg.dirname.equals(name) && kg.graphVersion.equals(version)).head)
    }

    foundGraph match {
      case Success(value) => value
      case Failure(exception) => print(s"Couldn't retrieve graph $name $version : ${exception.getMessage}")
        throw exception
    }
  }

  /** finds graphs with given RDF property or class list
   *
   * @param list list of classes or properties to look for
   * @return A Vector of KnowledgeGraphs having the given properties or classes
   */
  def getGraphsWithSchemaItem(list: Seq[GraphSchemaItem]): Vector[KnowledgeGraph] = {
    getLatestGraphs
      .filter(kg => Try(Void(getGraphStats(kg.dirname, kg.graphVersion))).isSuccess)
      .map(kg => (kg, getGraphStats(kg.dirname, kg.graphVersion)))
      .map(tuple => (tuple._1, Void(tuple._2).hasMultipleItemSubstrings(list)))
      .filter(_._2)
      .map(_._1)
  }


  /** Gets instance graphs only, ie those without an ontology namespace defined in their JSON metadata file.
   * Gets all of them, including old versions.
   *
   * @return a Vector of instance KnowledgeGraphs
   */
  def getAllInstanceGraphs: Vector[KnowledgeGraph] = getAllGraphs.filter(_.ontology_namespace.isEmpty)

  /** Gets latest instance graphs only, ie those without an ontology namespace defined in their JSON metadata file
   *
   * @return a Vector of instance KnowledgeGraphs
   */
  def getLatestInstanceGraphs: Vector[KnowledgeGraph] = getLatestGraphs.filter(_.ontology_namespace.isEmpty)

  /** Gets ontologies only, ie graphs with an ontology namespace defined in their JSON metadata file.
   * Gets all of them, including old versions.
   *
   * @return a Vector of ontological KnowledgeGraphs
   */
  def getAllOntologies: Vector[KnowledgeGraph] = getAllGraphs.filter(_.ontology_namespace.nonEmpty)

  /** Gets latest ontologies only, ie graphs with an ontology namespace defined in their JSON metadata file
   *
   * @return a Vector of ontological KnowledgeGraphs
   */
  def getLatestOntologies: Vector[KnowledgeGraph] = getLatestGraphs.filter(_.ontology_namespace.nonEmpty)

  /** Gets every version of every graph available
   *
   * @return A vector of KG currently available in the data-lake
   */
  def getAllGraphs: Vector[KnowledgeGraph] = listMetadataFiles.map(fileStatus => KnowledgeGraph(fileStatus, fileSystem, dataDir))

  /** Gets all graphs, but not old ones
   *
   * @return Every graph currently present in the data-lake, latest versions only
   */
  def getLatestGraphs: Vector[KnowledgeGraph] =
    listLatestMetadataFiles.map(fileStatus => KnowledgeGraph(fileStatus, fileSystem, dataDir))

  /** Retrieves VoID calculation results (located in default directory returned by getVoidDefaultOutputDir)
   *
   * @param name    graph name
   * @param version graph version
   * @return A vector of VoID graphs, one per graph source file if the total number of triple exceeded the limit during calculation
   */
  def getGraphStats(name: String, version: String): String = {

    def getDirContent(path: Path): String = {
      fileSystem.listStatus(path)
        .map(status => fileSystem.open(status.getPath).readAllBytes())
        .reduce(_ ++ _)
        .map(_.toChar)
        .mkString
    }

    Try(getDirContent(new org.apache.hadoop.fs.Path(getVoidDefaultOutputDir(name, version)))).getOrElse("")

  }


  /** List specific files generated during ingestion, each one corresponding to a graph.
   *
   * @return a vector of fileStatus "pointers" to every *_metadata_* file associated with a graph in the data repository
   */
  private def listMetadataFiles: Vector[FileStatus] =
    fileSystem.listStatus(dataPath)
      .filter(fileStatus => fileStatus.isFile)
      .filter(fileStatus => fileStatus.getPath.toString.contains("_metadata_")).toVector

  /** List specific files generated during ingestion, each one corresponding to a graph, lates version only
   *
   * @return a vector of fileStatus "pointers" to every *_metadata_latest* file associated with a graph in the data repository
   */
  private def listLatestMetadataFiles: Vector[FileStatus] =
    fileSystem.listStatus(dataPath)
      .filter(fileStatus => fileStatus.isFile)
      .filter(fileStatus => fileStatus.getPath.toString.endsWith("_metadata_latest")).toVector


  /** Standard outputdir for Spark jobs
   *
   * @param graphName    graph name
   * @param graphVersion graph version
   * @param jobType      subdir name. e.g. void, or qa
   * @return complete path of the output directory
   */
  private def getDefaultOutputDir(graphName: String, graphVersion: String, jobType: String): String = {
    val graph = getGraph(graphName, graphVersion)
    graph.graphDirPath + analysisDirNameSuffix + jobType
  }

  /** Returns specific output directory for VoID data
   *
   * @param graphName    graph name
   * @param graphVersion graph version
   * @return complete path of the void output directory
   */
  def getVoidDefaultOutputDir(graphName: String, graphVersion: String): String = {
    getDefaultOutputDir(graphName, graphVersion, voidDirName)
  }

  def getTime: String = {
    val timeInMillis = System.currentTimeMillis()
    val instant = Instant.ofEpochMilli(timeInMillis)
    val zonedDateTimeUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
    val dateTimeFormatter = DateTimeFormatter.ISO_ZONED_DATE_TIME
    dateTimeFormatter.format(zonedDateTimeUtc)
  }

  /** Logs general information in the log file specified by the config file (see constructor). Useful to trace Spark jobs.
   *
   * @param s String to append write
   */
  def log(s: String, logFileName: String = logFileName, logTime: Boolean = false): Unit = synchronized {


    val time = getTime

    if (logTime) println(s"$time: $s") else println(s)

    val hdfsPath = new Path(logFileName)

    def getStream: FSDataOutputStream = {
      if (fileSystem.exists(hdfsPath))
        fileSystem.append(hdfsPath)
      else
        fileSystem.create(hdfsPath)
    }

    @tailrec
    def writeWithRetry(retries: Int): Unit = {
      Try(getStream) match {
        case Success(stream) =>
          Try {
            if (logTime) stream.writeBytes(s"$time : $s\n") else stream.writeBytes(s"$s\n")
            stream.close()
          } match {
            case Success(_) => // Log written successfully
            case Failure(e) =>
              println(s"Failed to write log: $e")
              if (retries > 0) {
                Thread.sleep(100) // wait before retry
                writeWithRetry(retries - 1)
              }
          }
        case Failure(e) =>
          println(s"Failed to get stream: $e")
          if (retries > 0) {
            Thread.sleep(100) // wait before retry
            writeWithRetry(retries - 1)
          }
      }
    }

    writeWithRetry(3) // Retry up to 3 times
  }
}


