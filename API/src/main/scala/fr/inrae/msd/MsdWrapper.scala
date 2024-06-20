package fr.inrae.msd

//import net.sansa_stack.rdf.spark.io._
//import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.hadoop.fs.FileSystem

import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sbtBuildInfo.BuildInfo

/**
 * Wrapper factory. MsdWrapper class init uses non serializable classes which may throw org.apache.spark.SparkException: Task not serializable
 * * so we hold the class in a factory _object_ that will be copied with the uber jar to the nodes, consequently doesn't need to be serialized anymore
 */
object MsdWrapper {
  def apply(configFile: String = "/usr/local/msd-database-management/msdQuery.conf")= new MsdWrapper(configFile)
}

/** Wraps essential tools for data lake users.
 * Gives access to a Datalake object (with getGraphs, etc.), provides getTriples and getSubGraphTriples functions to load graphs as RDD[Triple]
 *
 * @param configFile Path to the config file (see example file src/main/resources/msdQuery.conf), with default to the standard one.
 */
class MsdWrapper(configFile: String = "/usr/local/msd-database-management/msdQuery.conf") {

  val dataLake: DataLake = new DataLake(configFile)
  val apiVersion: String = BuildInfo.version


  /** loads a graph into an RDD of Triples
   *
   * @param graphName    name of the graph, such as spar_cito
   * @param graphVersion optional graph version (set to "" for latest version)
   * @return the requested RDD of triples
   */
  def getTriples(graphName: String, graphVersion: String = ""): RDD[graph.Triple] = {
    MsdSparkTools(configFile).getGraphContent(graphName, graphVersion)
  }


  /** loads a part of graph into an RDD of Triples
   *
   * @param graphName    name of the graph, such as spar_cito
   * @param graphVersion optional graph version (set to "" for latest version)
   * @param fileList     a seq of HDFS file paths, such as ["/data/pubchem_synonym/file1.nt","/data/pubchem_synonym/file2.nt"].
   *                     Datalake.getGraph(name).getfiles() may be used to obtain the list.
   * @return the requested RDD of triples
   */
  def getSubGraphTriples(graphName: String, graphVersion: String = "", fileList: Seq[String]): RDD[graph.Triple] = {
    MsdSparkTools(configFile).getGraphContent(graphName, graphVersion, fileList)
  }


}
