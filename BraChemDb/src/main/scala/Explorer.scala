package fr.inrae.brachemdb


//import fr.inrae.msd.GraphXTools.{findNodeId, searchFromObject, searchFromSubject}

import fr.inrae.msd._
import net.sansa_stack.query.spark.sparqlify._
import net.sansa_stack.rdf.spark.model.GraphLoader
import net.sansa_stack.rdf.spark.model.graph.GraphOps.{constructGraph, constructHashedGraph}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

object Explorer {

  // Define constants for configuration keys and values
  private val KRYO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  private val KRYO_REGISTRATOR = "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"
  private val INITIAL_KRYO_BUFFER = "128m"
  private val MAX_KRYO_BUFFER = "2040m"
  private val CROSS_JOIN_ENABLED = "true"
  private val N_SHUFFLE_PARTITIONS = "250"
  private val ADAPTATIVE_ENABLED = "false"
  private val DYNAMIC_ALLOCATION_ENABLED = "false"



  def runExploration(outputDir: String, configFile: String, testContext: Boolean = false, nClusterCores: Int, manualPartition: Boolean): String = {

    val targetTaxon = "http://purl.obolibrary.org/obo/NCBITaxon_3700"
    val minimalSubclassNumber = 5000

    val outputFile = outputDir.stripSuffix("/") + "/brachemdbOutput-" + System.currentTimeMillis()
    val logFile = outputFile + ".log"
    val tempOutputDir = outputDir.stripSuffix("/") + "/brachemdbOutput-" + System.currentTimeMillis() + ".temp"

    @transient val spark: SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val f = Follower()

    f.log(s"#Starting search for compounds related to $targetTaxon , " + f.getTime, logFile)
    f.log(s"#Filename: $logFile", logFile)


    val wrapper = MsdWrapper(configFile)

    //1: find ancestor of http://purl.obolibrary.org/obo/NCBITaxon_3700 (Brassicaceae)
    val taxonTriples: RDD[Triple] = wrapper.getTriples("ncbitaxon").repartition(8)//.cache() //8 partitions -> approx 192M/partition

    //search ancestors, ie objects of property "subClassOf", starting from subject NCBITaxon_3700
    //val superTaxon = GraphXTools.searchPropertySubGraph(taxonTriples, "http://www.w3.org/2000/01/rdf-schema#subClassOf", fromSubject = true, targetTaxon, 1)
    //val superTaxonUri = superTaxon.take(1).map(_.getURI).apply(0)
    val superTaxonUri = "http://purl.obolibrary.org/obo/NCBITaxon_3699"

    //search descendants, ie subjects of property "subClassOf", starting from subject "superTaxon""
    val taxaSubGraphElementsRDD = GraphXTools.searchPropertySubGraphAsGraph(
      taxonTriples,
      "http://www.w3.org/2000/01/rdf-schema#subClassOf",
      fromSubject = false,
      superTaxonUri,
      maxIterations = 15)

    //Had some strange behavior during tests without making a new RDD from scratch
    val taxaSubGraphElements = if (testContext) spark.sparkContext.parallelize(taxaSubGraphElementsRDD.collect()) else taxaSubGraphElementsRDD
    val nTaxa = taxaSubGraphElementsRDD.count()
    f.log(s"#Taxonomy exploration led to $nTaxa taxa.", logFile)
    if(nTaxa < minimalSubclassNumber) {
      f.log(s"#Found less than $minimalSubclassNumber, stopping.", logFile)
      throw new Exception(s"Found less than $minimalSubclassNumber, stopping.")
    }



    val refTriples: RDD[Triple] = Seq("ncbitaxon", "nlm_mesh", "nlm_mesh_ontology", "pubchem_reference", "pubchem_substance").map {
      db => wrapper.getTriples(db)
    }.reduce((t1, t2) => t1.union(t2))

    val cachedArticleTriples: RDD[Triple] = refTriples
      .union(taxaSubGraphElements) //we add the result of the Pregel exploration in order to use it in the SPARQL request
      .repartition(250) //approx 128M/partition
      //.persist(StorageLevel.MEMORY_ONLY_2)
      //.cache()

    val resultBindings1: RDD[Binding] = execQuery(articlesQuery("fabio:hasSubjectTerm"), cachedArticleTriples)//.persist(StorageLevel.MEMORY_ONLY_2)
    val resultBindings2: RDD[Binding] = execQuery(articlesQuery("fabio:hasPrimarySubjectTerm"), cachedArticleTriples)//.persist(StorageLevel.MEMORY_ONLY_2)
    val resultBindings = resultBindings1.union(resultBindings2)
    implicit val ssEnco: Encoder[(String, String, String, String)] = Encoders.kryo(classOf[(String, String, String, String)])

    val pmids_compounds = resultBindings.map(binding => (binding.get("pmid"), binding.get("pmidTitle"), binding.get("compound"), binding.get("cousinId")))

    val result = pmids_compounds.map(_._3)
      .distinct
      .map(r => s"<${r.getURI}> <http://www.w3.org/2004/02/skos/core#related> <$targetTaxon>")


    writeDFToSingleFile(result.toDF(), fileSystem, tempOutputDir, outputFile)


    //log detailed results as comments
    f.log("#Source articles sorted by compound :", logFile)
    pmids_compounds.sortBy(_._3.toString()).map("#" + _.toString()).collect().foreach(f.log(_, logFile))
    f.log(s"#End of results for compounds related to $targetTaxon. ", logFile)

    //    val taxa = taxaSubGraphElements.map(t => t.getMatchSubject.toString)
    //      .map(_.replace("http://purl.obolibrary.org/obo/NCBITaxon_", ""))
    //      .collect()
    //      .reduce(_ + " " + _)
    //    f.log(s"#This exploration is based on ${taxa.length} taxa under $superTaxonUri :", logFile)
    //    f.log(s"# $taxa", logFile)

    spark.stop()
    outputFile
  }

  def writeDFToSingleFile(df: DataFrame, hdfs: FileSystem, tempOutputPath: String, finalOutputPath: String): Boolean = {

    // Coalesce to a single partition
    val singlePartitionDF = df.coalesce(1)

    // Write DataFrame to HDFS in CSV format to the temporary directory
    singlePartitionDF.write
      .mode("overwrite")
      .option("header", "false")
      .csv(tempOutputPath)

    // Get the part file written by Spark
    val partFile = hdfs.globStatus(new Path(s"$tempOutputPath/part-*"))(0).getPath

    // Rename part file to the final single file
    hdfs.rename(partFile, new Path(finalOutputPath))

    // Delete the temporary directory
    hdfs.delete(new Path(tempOutputPath), true)
  }


  private def articlesQuery(fabioProperty: String): String =
    s"""PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
       |PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
       |PREFIX fabio: <http://purl.org/spar/fabio/>
       |PREFIX cito: <http://purl.org/spar/cito/>
       |PREFIX sio: <http://semanticscience.org/resource/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |PREFIX ncbiTaxon: <http://purl.obolibrary.org/obo/>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |
       |# Select distinct values for PubMed ID, PubMed Title, and Compound
       |SELECT DISTINCT ?pmid ?pmidTitle ?compound ?cousinId {
       |
       |  ?cousinId <http://inrae.fr/propertySubGraphElementOf> ?parent .
       |
       |  # Find cousin's labels
       |  ?cousinId rdfs:label ?cousinLabel .
       |
       |  # Find cousin's MeSH descriptor. TODO: search terms, pref. terms etc
       |  ?d rdfs:label ?cousinLabel .
       |  ?d a meshv:TopicalDescriptor .
       |
       |  # Connect PubMed articles to MeSH descriptors
       |  # using the FABIO property specified in the data
       |  ?pmid $fabioProperty ?d .
       |
       |  # Get the title of each PubMed article
       |  ?pmid dcterms:title ?pmidTitle .
       |
       |  # Connect substances to articles using CITO ontology
       |  ?substance cito:isDiscussedBy ?pmid .
       |
       |  # Connect substances to compounds using the SIO ontology
       |  ?substance sio:CHEMINF_000477 ?compound .
       |}
       |""".stripMargin

  private def execQuery(queryS: String, triples: RDD[Triple]): RDD[Binding] = {
    @transient val spark: SparkSession = SparkSession.builder().getOrCreate()

    new QueryEngineFactorySparqlify(spark)
      .create(triples)
      .createQueryExecution(queryS)
      .execSelectSpark()
      .getBindings
  }

  /** Checks the taxonomy exploration (up then down) returns more than 76000 taxa under Brassicales
   *
   * @param outputDir  log output dir
   * @param spark      sparkSession
   * @param configFile MSDLib configfile
   */
  def selfTest(outputDir: String, configFile: String): Unit = {

    @transient val spark: SparkSession = SparkSession.builder().getOrCreate()

    val targetTaxon = "http://purl.obolibrary.org/obo/NCBITaxon_3700"
    val millis = System.currentTimeMillis()

    val outputFile = outputDir.stripSuffix("/") + "/brachemdbTest-" + millis
    spark.sparkContext.setLogLevel("DEBUG")
    val f = Follower()
    f.log(s"#Starting search for compounds related to $targetTaxon , " + f.getTime, outputFile)
    f.log(s"#Filename: $outputFile", outputFile)
    val wrapper = MsdWrapper(configFile)

    f.log(s"#Loading taxonomy... , " + f.getTime, outputFile)
    val taxonTriples: RDD[Triple] = wrapper.getTriples("ncbitaxon").repartition(8)

    val graphXTools = GraphXTools

    val superTaxon = graphXTools.searchPropertySubGraph(taxonTriples, "http://www.w3.org/2000/01/rdf-schema#subClassOf", fromSubject = true, targetTaxon, 1).collect()
    val superTaxonUri = superTaxon.take(1).map(_.getURI).apply(0)
    f.log(s"#Supertaxon is : $superTaxonUri, expected : http://purl.obolibrary.org/obo/NCBITaxon_3699", outputFile)
    if (superTaxonUri != "http://purl.obolibrary.org/obo/NCBITaxon_3699")
      throw new Exception(s"Taxonomy exploration failed : expected http://purl.obolibrary.org/obo/NCBITaxon_3699 above $targetTaxon, found $superTaxonUri")
    //val superTaxonUri = "http://purl.obolibrary.org/obo/NCBITaxon_3699"


    val taxaSubGraphElementsRDD = graphXTools.searchPropertySubGraphAsGraph(
      taxonTriples,
      "http://www.w3.org/2000/01/rdf-schema#subClassOf",
      fromSubject = false,
      superTaxonUri,
      maxIterations = 15).cache()


    val size = taxaSubGraphElementsRDD.count()
    f.log(s"#This test found ${size} taxa under $superTaxonUri.", outputFile)
    val minSize = 5100 // actual size was 5161 the 4th of june 2024
    if (size < minSize) {
      f.log(s"#The min size is set to $minSize, the test failed.", outputFile)
      throw new Exception(s"Did not found enough taxa under $superTaxonUri. Minimum value is set to $minSize, only found $size")
    }


  }

  def fakeRun2(outputDir: String): Unit = {
    fakeRun(outputDir, 2)
  }

  def fakeRun1(outputDir: String): Unit = {
    fakeRun(outputDir, 1)
  }

  private def fakeRun(outputDir: String, rank: Int): Unit = {
    val outputFile = outputDir.stripSuffix("/") + "/brachemdbOutput-" + System.currentTimeMillis()
    val targetTaxon = "http://purl.obolibrary.org/obo/NCBITaxon_3700"
    val f = Follower()
    f.log(s"#Starting search for compounds related to $targetTaxon , " + f.getTime, outputFile)
    rank match {
      case 1 => f.log(f.fakeFirstResults, outputFile)
      case 2 => f.log(f.fakeLastResults, outputFile)

    }
  }


  def buildSparkSession(appName : String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      // Set the Kryo serializer for better performance with complex data structures
      .config("spark.serializer", KRYO_SERIALIZER)
      // Enable cross join
      .config("spark.sql.crossJoin.enabled", CROSS_JOIN_ENABLED)
      // Set the initial buffer size for Kryo serialization
      .config("spark.kryoserializer.buffer", INITIAL_KRYO_BUFFER)
      // Set the maximum buffer size for Kryo serialization
      .config("spark.kryoserializer.buffer.max", MAX_KRYO_BUFFER)
      // Register custom Kryo registrator
      .config("spark.kryo.registrator", KRYO_REGISTRATOR)
      // Optional: Set the number of shuffle partitions (default is 200)
      .config("spark.sql.shuffle.partitions", N_SHUFFLE_PARTITIONS)
      // Optional: Enable adaptive query execution
      .config("spark.sql.adaptive.enabled", ADAPTATIVE_ENABLED)
      // Optional: Enable dynamic allocation
      .config("spark.dynamicAllocation.enabled", DYNAMIC_ALLOCATION_ENABLED)

      .getOrCreate()
  }



}