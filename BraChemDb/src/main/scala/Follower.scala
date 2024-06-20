package fr.inrae.brachemdb


//import fr.inrae.msd.DataLake.{fileSystem, logFileName}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Follower extends Serializable {
  val f: Follower= new Follower()
  def apply(): Follower = {
    f
  }
}
/** Tool allowing to follow evolution of research results
 *
 */
class Follower extends Serializable {

  @transient lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  @transient lazy val fileSystem : FileSystem =  FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private val debug = false //using old debug methods because of miniDFSCluster incompatibility

  def makeDiff(outputDir: String, outputFileNameBasis: String, newResultsFileName: String): Unit = {

    val diffFile = new Path(outputDir.stripSuffix("/") + "/" + newResultsFileName)
    val diffLogFile = outputDir.stripSuffix("/") + "/" + newResultsFileName + ".diffLog"
    if (debug) log("Starting makeDiff with output params : diff to " + diffFile,diffLogFile)
    val completeResultsFile = new Path(diffFile.toString + "-complete")
    if (debug) log("and complete resutl to " + completeResultsFile,diffLogFile)

    //remove "new result available" flag file
    if (fileSystem.exists(diffFile)) {
      if (debug) log("Deleting prexisting diff  " + diffFile,diffLogFile)
      fileSystem.delete(diffFile, true)
    }

    if (fileSystem.exists(completeResultsFile)) {
      if (debug) log("Deleting prexisting res  " + completeResultsFile,diffLogFile)
      fileSystem.delete(completeResultsFile, true)
    }

    Thread.sleep(1000) //we wait before writing

    //create diff with 2 latest results
    if (debug) log("Now running checkForNewResults in " + outputDir + " with basis " + outputFileNameBasis,diffLogFile)

    val newResults = checkForNewResults(outputDir, outputFileNameBasis,diffLogFile)
    if (debug) {
      log(newResults._1.size + s" new results in " + newResults._2,diffLogFile)
      newResults._1.foreach(t=> log(t.toString(),diffLogFile))
    }
    if (newResults._1.nonEmpty) {
      log(s"#new results (difference only, see ${completeResultsFile.toString})", diffFile.toString)
      log(s"<${diffFile.toString}> <http://www.w3.org/ns/prov#wasDerivedFrom> <${newResults._2}>", diffFile.toString)
      newResults._1.foreach(t => log(s"<${t._1}> <${t._2}> <${t._3}>", diffFile.toString))
      //also copy the new results to a file we send
      FileUtil.copy(fileSystem, new Path(newResults._2), fileSystem, completeResultsFile, false, fileSystem.getConf)

    } else log("# NO newer results", diffFile.toString)

    if (debug) log("END OF DIFF FUNC",diffLogFile)


  }


  def checkForNewResults(outputDir: String, resultFileNameBasis: String, diffLogFile: String): (Set[(String, String, String)], String) = {

    val resultDir = outputDir.stripSuffix("/")

    if (debug) {
      log("Looking for new versions in " + resultDir,diffLogFile)
      log("Found files :",diffLogFile)
    }
    val files = fileSystem.listStatus(new Path(resultDir))
      .filter(status => status.isFile && status.getPath.toString.contains(resultFileNameBasis))
      .map(status => status.getPath.toString)
    if (debug) {files.foreach(log(_,diffLogFile))

    }


    if (debug) log("End of files are : ",diffLogFile)
    val strings = files.map(getEndOfString(_, resultDir + "/" + resultFileNameBasis))
    if (debug) strings.foreach(log(_,diffLogFile))

    if (debug) log("Converting end of files to longs. : ",diffLogFile)
    val longs = strings.flatMap(millis => Try(millis.toLong).toOption)
    if (debug) longs.foreach(n=>log(n.toString,diffLogFile))


    if (debug) log("Taken longs = ",diffLogFile)
    val versions = longs.sorted(Ordering[Long].reverse).take(2)
    if (debug) {
      log("Found " + versions.length + " versions : ",diffLogFile)
      versions.foreach(n=> log(n.toString,diffLogFile))
      log("Done versions. Thread : " + Thread.currentThread().getId,diffLogFile)
    }


    if (versions.nonEmpty && versions.length > 1) {
      val newResult = resultDir + "/" + resultFileNameBasis + versions(0)
      val oldResult = resultDir + "/" + resultFileNameBasis + versions(1)
      (compareResults(oldResult, newResult), resultDir + "/" + resultFileNameBasis + versions(0))
    } else if (versions.length == 1) {
      val newResult = resultDir + "/" + resultFileNameBasis + versions(0)
      if (debug) {
        log("1 version present. Results from " + newResult,diffLogFile)
        fileContentToCompounds(readFile(newResult)).foreach(t=>log(t.toString(),diffLogFile))
      }
      (fileContentToCompounds(readFile(newResult)), newResult)
    } else (Set.empty[(String, String, String)], "noResultsNoFile") //TODO use an Option


  }


  private def compareResults(precedentResultPath: String, currentResultPath: String): Set[(String, String, String)] = {
    val contentBefore = fileContentToCompounds(readFile(precedentResultPath))
    val contentAfter = fileContentToCompounds(readFile(currentResultPath))
    contentAfter.diff(contentBefore)
  }

  def readFile(path: String): String = {
    val p = new org.apache.hadoop.fs.Path(path)
    fileSystem.open(p).readAllBytes()
      .map(_.toChar)
      .mkString
  }

  private def parseLine(line: String): Option[(String, String, String)] = {
    val pattern = """<([^>]+)> <([^>]+)> <([^>]+)>""".r
    val trimmedLine = line.trim
    trimmedLine match {
      case pattern(compound, pred, taxon) =>
        Some((compound, pred, taxon))
      case s: String =>
        None
    }
  }

  private def fileContentToCompounds(content: String): Set[(String, String, String)] = {
    content.split("\n")
      .filter(_.nonEmpty)
      .flatMap(parseLine)
      .toSet
  }


  def getEndOfString(str: String, middle: String): String = {
    val index = str.indexOf(middle)
    if (index != -1) str.substring(index + middle.length) else ""
  }

  def getTime: String = {
    val timeInMillis = System.currentTimeMillis()
    val instant = Instant.ofEpochMilli(timeInMillis)
    val zonedDateTimeUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
    val dateTimeFormatter = DateTimeFormatter.ISO_ZONED_DATE_TIME
    dateTimeFormatter.format(zonedDateTimeUtc)
  }

  //todo : remove and use Datalake log
  def log(s: String, logFileName: String, logTime: Boolean = false): Unit = synchronized {


    val time = getTime

    if (logTime) println(s"$time: $s")  else println(s)

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

  def fakeFirstResults: String =
    """
      |#Graph size estimated to 24440 bytes
      |#Using 1 partition(s)
      |#Partition size : 24440 bytes each
      |
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID398553> <http://www.w3.org/2004/02/skos/core#related> <http://purl.obolibrary.org/obo/NCBITaxon_3700>
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID10494> <http://www.w3.org/2004/02/skos/core#related> <http://purl.obolibrary.org/obo/NCBITaxon_3700>
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713> <http://www.w3.org/2004/02/skos/core#related> <http://purl.obolibrary.org/obo/NCBITaxon_3700>
      |
      |#Results obtained from :
      |#(http://rdf.ncbi.nlm.nih.gov/pubchem/reference/24720599,"Bioactive crude plant seed extracts from the NCAUR oilseed repository",http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID10494)
      |#(http://rdf.ncbi.nlm.nih.gov/pubchem/reference/11363759,"In vitro Antimicrobial Comparison of Chlorhexidine, Persica Mouthwash and Miswak Extract",http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713)
      |#(http://rdf.ncbi.nlm.nih.gov/pubchem/reference/8662943,"Ion release from orthodontic brackets in 3 mouthwashes: An in-vitro study",http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713)
      |
      |""".stripMargin


  def fakeLastResults: String =
    """
      |#Graph size estimated to 24440 bytes
      |#Using 1 partition(s)
      |#Partition size : 24440 bytes each
      |
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID398553> <http://www.w3.org/2004/02/skos/core#related> <http://purl.obolibrary.org/obo/NCBITaxon_3700>
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID10494> <http://www.w3.org/2004/02/skos/core#related> <http://purl.obolibrary.org/obo/NCBITaxon_3700>
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713> <http://www.w3.org/2004/02/skos/core#related> <http://purl.obolibrary.org/obo/NCBITaxon_3700>
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID8082> <http://www.w3.org/2004/02/skos/core#related> <http://purl.obolibrary.org/obo/NCBITaxon_3700>
      |
      |#Results obtained from :
      |#(http://rdf.ncbi.nlm.nih.gov/pubchem/reference/24720599,"Bioactive crude plant seed extracts from the NCAUR oilseed repository",http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID10494)
      |#(http://rdf.ncbi.nlm.nih.gov/pubchem/reference/11363759,"In vitro Antimicrobial Comparison of Chlorhexidine, Persica Mouthwash and Miswak Extract",http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713)
      |#(http://rdf.ncbi.nlm.nih.gov/pubchem/reference/8662943,"Ion release from orthodontic brackets in 3 mouthwashes: An in-vitro study",http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713)
      |#(http://rdf.ncbi.nlm.nih.gov/pubchem/reference/11884857,"Profiling Glucosinolates, Flavonoids, Alkaloids, and Other Secondary Metabolites in Tissues of Azima tetracantha L. (Salvadoraceae)",http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID8082))
      |
      |""".stripMargin

}