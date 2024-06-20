package fr.inrae.msd

import scala.util.{Failure, Success, Try}
import sbtBuildInfo.BuildInfo

/** Holds a set of functions provided to explore the datalake in command line interface and demonstrate the use of the API.
 *
 * @param configFile File storing Datalake params. See example file src/main/resources/msdQuery.conf
 */
class InteractiveFunctions(configFile: String) {
  val apiVersion: String = BuildInfo.version
  private val dataLake: DataLake = new DataLake(configFile,true)

  /** Prints graphs containing a given list of classes or properties
   *
   * @param list        the list of properties or classes
   */
  def findAndPrintSchemaItems(list: Seq[GraphSchemaItem]): Unit = {

    def printDetails(g: KnowledgeGraph, items: Seq[GraphSchemaItem]): Unit = {
      val void = Void(dataLake.getGraphStats(g.dirname, g.graphVersion))
      val (partition,name) = items match {
        case Seq(_: ClassItem, _*)=> (void.getClassPartition, "Class")
        case Seq( _: PropertyItem, _*) => (void.getPropertyPartition, "Property")
      }

      partition.filterKeys(key => items.map(_.name).exists(key.toString.contains))
        .map(m => s"$name ${m._1} found ${m._2} time(s)").foreach(println)
      println()
    }

    //TODO retrieve also old versions ?
    val graphs = dataLake.getGraphsWithSchemaItem(list)

    val s1 = if (graphs.size > 1) "s" else ""
    val s2 = if (list.size> 1) "s" else ""

    print(s"\n${graphs.size} graph$s1 containing following item$s2: \n")
    list.foreach(s => print(s + " "))
    println("\n")

    graphs.foreach(g => {
      println(s"${g.dirname} ${g.graphVersion} (located in ${g.graphDirPath})")
      printDetails(g, list)
    })
  }

  /** Prints the VoID stats of the given graph in Turtle
   *
   * @param name    graph name
   * @param version graph version
   */
  def graphStats(name: String, version: String): Unit = {
    println(dataLake.getGraphStats(name, version))
  }


  /**Prints a list of graphs
   * pubchem_substance 2024-02-03, TURTLE, 2090173367 triples, instance graph
   * pubchem_synonym 2024-02-03, TURTLE, 525613807 triples, instance graph
   * sem_chem 2.0, RDFXML, 2866 triples, ontology
   *
   * Number of graphs listed : 23
   *
   * @param v vector holding graphs
   * @param printSize whether to print graphs sizes or not (requires VoID stats)
   */
  private def gPrint(v: Vector[KnowledgeGraph], printSize: Boolean = false): Unit = {
    def simplePrint(g: KnowledgeGraph) = s"${g.dirname} ${g.graphVersion}, ${g.strRdfFormat}"
    def printWithSize(g: KnowledgeGraph) = {
      val size = Void(dataLake.getGraphStats(g.dirname, g.graphVersion)).getGraphSize
      val sizeInfo = if(size > 10000) f", ${size.toFloat}%.2e triples" else if (size !=0 ) s", $size triples" else ""
      val ontoOrInstance = if(g.ontology_namespace !="") ", ontology" else ", instance graph"
      simplePrint(g) +  sizeInfo  + ontoOrInstance
    }
    v.map(g => if (printSize) printWithSize(g) else simplePrint(g) ).foreach(println)
    println(s"\nNumber of graphs listed: ${v.length}")
  }

  /** Prints the list of RDF graphs
   *
   * @param all old versions will also be listed if set to true
   */
  def listAllGraphs(all: Boolean = true): Unit = {
    if (all)
      gPrint(dataLake.getAllGraphs)
    else
      gPrint(dataLake.getLatestGraphs)
  }

  /** Prints the content of the JSON file generated during the graph ingestion
   *
   * @param name    graph name, corresponding to dir_name in the Airflow DAG settings
   * @param version optional version to get an older graph's metadata
   */
  def metadata(name: String, version: String = ""): Unit = {
    println(Try(interactiveGetGraph(name, version).jsonMetadata.toString()).getOrElse("Error"))
  }

  /** Prints Hadoop URIs of RDF files for a given graph
   *
   * @param name    graph name, corresponding to dir_name in the Airflow DAG settings
   * @param version optional version to get an older graph's files URIs
   */
  def listFilesUris(name: String, version: String = ""): Unit = {
    println(Try(interactiveGetGraph(name, version).getFilesUris.reduce(_ + "\n" + _)).getOrElse("Error"))
  }

  /** Prints HDFS absolute paths of RDF files for a given graph
   *
   * @param name    graph name, corresponding to dir_name in the Airflow DAG settings
   * @param version optional version to get an older graph's files paths
   */
  def listFilesPaths(name: String, version: String = ""): Unit = {
    println(Try(interactiveGetGraph(name, version).getFiles.reduce(_ + "\n" + _)).getOrElse("Error"))
  }

  /** Prints names of RDF files for a given graph
   *
   * @param name    graph name, corresponding to dir_name in the Airflow DAG settings
   * @param version optional version to get an older graph's files names
   */
  def listFilesNames(name: String, version: String = ""): Unit = {
    println(Try(interactiveGetGraph(name, version).graphFileList.reduce(_ + "\n" + _)).getOrElse("Error"))
  }

  /** Handles possible exception during graph fetching
   *
   * @param name    graph name, corresponding to dir_name in the Airflow DAG settings
   * @param version optional version to get an older graph
   * @return the corresponding KnowledgeGraph object if found, otherwise prints an error message and propagates the exception.
   */
  private def interactiveGetGraph(name: String, version: String = ""): KnowledgeGraph = {

    def meta: Try[KnowledgeGraph] = Try(dataLake.getGraph(name, version))

    meta match {
      case Success(v) =>
        v
      case Failure(e) =>
        println(s"Error getting graph $name  (${e.getMessage})")
        throw e
    }
  }

  /**Prints the list of instance graphs loaded into the lake.
   *
   */
  def printInstanceGraphs(): Unit = {
      gPrint(dataLake.getLatestInstanceGraphs)
  }


  /**Prints the list of ontologies loaded into the lake.
   *
   */
  def printOntologies(): Unit = {
    gPrint(dataLake.getLatestOntologies)
  }

  /**Sums graphs sizes (in triples)
   * @return a String holding total sizes in scientific notation, ready to print
   */
  private def datalakeTripleNumber(): String = {
    def totalSize(s: Seq[KnowledgeGraph]): Double = Try(s.map(g => Void(dataLake.getGraphStats(g.dirname, g.graphVersion)).getGraphSize).sum.toDouble).getOrElse(-1.0)
    val ontologiesSize = totalSize(dataLake.getLatestOntologies)
    val instanceSize = totalSize(dataLake.getLatestInstanceGraphs)


    f"""Latest ontologies total: $ontologiesSize%.2e triples
       |Latest instance graphs total: $instanceSize%.2e triples
       |Global: ${ontologiesSize+instanceSize}%.2e triples""".stripMargin

  }

  /**Prints global dataLake information. Example :
   *
   * pubchem_synonym 2024-02-03, TURTLE, 525613807 triples, instance graph
   * pubchem_void 2024-02-03, TURTLE, 43554 triples, instance graph
   * sem_chem 2.0, RDFXML, 2866 triples, ontology
   * [...]
   *
   * Number of graphs listed : 23
   *
   * Latest ontologies total: 2.43e+07 triples
   * Latest instance graphs total: 7.64e+09 triples
   * Global : 7.67e+09 triples
   *
   */
  def printGraphSizes(): Unit = {
    gPrint(dataLake.getLatestGraphs, printSize = true)
    println
    println(datalakeTripleNumber())
  }


}
