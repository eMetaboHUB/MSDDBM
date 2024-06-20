package fr.inrae.msd

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.util.{Failure, Success}
//import org.apache.jena.riot.Lang
import scala.util.Try
//import upickle.default._
import sbtBuildInfo.BuildInfo

/**
 * This class represents a knowledge graph available on the datalake.
 *
 * @param fileSystem    HDFS file system containing the graph
 * @param dirname       base of the name of the directory containing the graphs' files, as "chebi", corresponding to the dir_name parameter of the Airflow DAG.
 * @param graphVersion  graph version, as "2023-11-01"
 * @param graphDirPath  directory pathname, like "/data/chebi_v2023-11-01"
 * @param graphFileList Vector containing the graph's files names, eg. Vector["chebi.owl",]
 * @param strRdfFormat  RDF format of the graph, eg "TURTLE" , "NTRIPLES", "RDFXML". See  https://jena.apache.org/documentation/io/rdf-input.html for a comprehensive list.
 * @param jsonMetadata  Full content of the metadata file created during the graph ingestion
 */
case class KnowledgeGraph(fileSystem: FileSystem,
                          dirname: String,
                          graphVersion: String,
                          graphDirPath: String,
                          graphFileList: Vector[String],
                          strRdfFormat: String,
                          ontology_namespace: String,
                          jsonMetadata: ujson.Value,
                          ) {

  val apiversion: String = BuildInfo.version

  /** Lists the absolute paths of the graph's files, such as "/data/skos_v2011-08-06/skos.rdf"
   * @return a Vector containing the graph files hdfs path
   */
  def getFiles: Vector[String] = {
    graphFileList.map(file => graphDirPath + "/" + file)
  }

  /** Lists the absolute paths of the graph's file's URIs, such as hdfs://147.100.175.223:9000/data/skos_v2011-08-06/skos.rdf
   * @return a Vector containing the graph files complete uris
   */
  def getFilesUris: Vector[String] = {
    //todo test
    graphFileList.map(file => fileSystem.getUri.toString + graphDirPath + "/" + file)
  }

  /**Gets graph's files opened as InputStreams
   * @return  a Vector of already open graphs as InputStreams
   */
  def getFilesInputStreams : Vector[java.io.InputStream] = {
    getFilesUris.map(url => fileSystem.open(new org.apache.hadoop.fs.Path(url)).getWrappedStream)
  }
}


/** Companion object used to build a KG from its JSON representation built during ingestion.
 *
 */
object KnowledgeGraph {

  /**
   * @param status HDFS status of the JSON file to be parsed
   * @param fileSystem the HDFS
   * @param dataDir the path to data
   * @return a ready-to use KG object
   */
  //TODO : add more info, like graph name and desc.
  def apply(status: FileStatus, fileSystem: FileSystem, dataDir: String): KnowledgeGraph = {
    val json: ujson.Value = Try {
      val hdfsStream = fileSystem.open(status.getPath)
      val res = ujson.read(hdfsStream)
      hdfsStream.close()
      res
    } match {
      case Success(value) => value
      case Failure(exception) =>
        val message = (s"Failed to read JSON from HDFS while parsing ${status.getPath} : ${exception.getMessage}")
        throw new Exception(message)
    }

    val dirname = Try(json("dir_name").str).getOrElse("NoDirNameSpecified")
    val version = Try(json("version").str).getOrElse("NoVersionSpecified")
    val path = dataDir + "/" + dirname + "_v" + version
    val fileList = Try(json("actual_file_list").arr.map(f => f.str).toVector).getOrElse(Vector("NoFileListSpecified"))
    val rdfFormat = Try(json("format").str) getOrElse "NoFormatSpecified"
    val ontology_namespace = Try(json("ontology_namespace").str).getOrElse("")  //.str.stripSuffix("/").stripSuffix("#")) getOrElse ""


    new KnowledgeGraph(
      fileSystem,
      dirname,
      version,
      path,
      fileList,
      rdfFormat,
      ontology_namespace,
      json,
    )
  }

}