import fr.inrae.msd.{DataLake, GraphXTools, MsdSparkTools, MsdWrapper, PropertyItem, TestHelper, TripleCase, Void}
import org.apache.hadoop.fs.Path
import org.apache.jena.rdf.model.{ModelFactory, ResourceFactory}
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, FileNotFoundException}
import scala.util.Try

/** WARNING : these tests wont pass with RDFXML graphs : function "def rdfxml: String => RDD[Triple]" in
 * https://github.com/SANSA-Stack/SANSA-Stack/blob/6452488253c362761c46a54ac6a7e43e9f00f04d/sansa-rdf/sansa-rdf-spark/src/main/scala/net/sansa_stack/rdf/spark/io/package.scala#L4
 * line 450
 * seems to alter hadoop config and mess with the miniDFSCluster used during tests
 *
 */
class MsdSparkToolsAndApiTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testHelper: TestHelper = TestHelper("./target/hdfs-spark/")
  private val dataLake = new DataLake(testHelper.generatedConfigFileName)


  test("console toParquet"){
    testHelper.create1MockGraphs2versions(testHelper.millis)
    fr.inrae.msd.Main.main(Array("toParquet", "-n", "testGraph", "-v",testHelper.millis, "--maxTriplesPerFile", "2", "--sparkStop", "false", "-c", testHelper.generatedConfigFileName))
    assert(testHelper.testFS.listStatus(new Path(dataLake.getGraph("testGraph").graphDirPath + ".parquet")).length == 4) //expect 3 data files + 1 _SUCCESS

    val tools = MsdSparkTools(testHelper.generatedConfigFileName)
    val loadedFromParquet = tools.getGraphContent(testHelper.defaultMockGraphName, testHelper.millis,forceParquet = true)

    val triples = """http://example.org/subject2 @http://example.org/predicate "Object 2"
                    |http://example.org/subject3 @http://example.org/predicate "Object 3"
                    |http://example.org/subject1 @http://example.org/predicate "Object 1"""".stripMargin.split("\n")

    assert (loadedFromParquet.count() == 3)
    loadedFromParquet.foreach(t=>assert(triples.contains(t.toString)))
  }

  test("graph to Parquet") {
    testHelper.create1MockGraphs2versions(testHelper.millis)

    val tools = MsdSparkTools(testHelper.generatedConfigFileName)

    assertThrows[FileNotFoundException](tools.getGraphContent(testHelper.defaultMockGraphName, testHelper.millis,forceParquet = true))

    tools.serializeGraphToParquet(testHelper.defaultMockGraphName, testHelper.millis)

    val loadedFromParquet = tools.getGraphContent(testHelper.defaultMockGraphName, testHelper.millis,forceParquet = true)

    val triples = """http://example.org/subject2 @http://example.org/predicate "Object 2"
                    |http://example.org/subject3 @http://example.org/predicate "Object 3"
                    |http://example.org/subject1 @http://example.org/predicate "Object 1"""".stripMargin.split("\n")

    assert (loadedFromParquet.count() == 3)
    loadedFromParquet.foreach(t=>assert(triples.contains(t.toString)))

  }

  test("Save and retrieve with Parquet") {
    testHelper.create1MockGraphs2versions(testHelper.millis)

    val tools = MsdSparkTools(testHelper.generatedConfigFileName)

    val originalContentRdd = tools.getGraphContent(testHelper.defaultMockGraphName, testHelper.millis)

    tools.saveTriplesAsParquet(originalContentRdd,testHelper.testDir+ "/testGraph.parquet")

    val loadedContent = tools.loadTriplesFromParquet(testHelper.testDir+ "/testGraph.parquet")


    assert (originalContentRdd.count == loadedContent.count())

    import org.apache.jena.graph.Triple

    def toTripleCase(rdd:RDD[Triple]):Array[TripleCase] = {
      rdd.map(triple => TripleCase(
        triple.getSubject.toString,
        triple.getPredicate.toString,
        triple.getObject.toString
      )).collect()
    }
    val original =toTripleCase(originalContentRdd)
    val loaded = toTripleCase(loadedContent)
    assert(original.map(loaded.contains(_)).reduce(_ && _))
  }


  test("Test Pregel exploration with output as graph (RDD of Triples)") {
    testHelper.createTaxonGraph()
    val msdWrapper = MsdWrapper(testHelper.generatedConfigFileName)
    val triples = msdWrapper.getTriples(testHelper.defaultMockGraphName, testHelper.millis)

    val outputGraph = GraphXTools.searchPropertySubGraphAsGraph(triples, "http://www.w3.org/2000/01/rdf-schema#subClassOf", fromSubject = false, "http://purl.obolibrary.org/obo/NCBITaxon_10")
    assert(outputGraph.count() == 5)
    assert(outputGraph.map(triple => triple.getPredicate.toString == "http://inrae.fr/propertySubGraphElementOf").reduce(_ && _))
    assert(outputGraph.map(triple => triple.getObject.toString == "http://purl.obolibrary.org/obo/NCBITaxon_10").reduce(_ && _))
    assert(outputGraph.map(triple => triple.getSubject.toString).collect().contains("http://purl.obolibrary.org/obo/NCBITaxon_100"))
    assert(outputGraph.map(triple => triple.getSubject.toString).collect().contains("http://purl.obolibrary.org/obo/NCBITaxon_200"))
  }

  test("Use searchPropertySubGraph to get all descendants and ancestors of a taxon within a cyclic graph ") {
    testHelper.createTaxonGraph(withCycle = true)
    val wrapper = MsdWrapper(testHelper.generatedConfigFileName)
    val triples = wrapper.getTriples(testHelper.defaultMockGraphName, testHelper.millis)

    //search ancestors, ie objects of property "subClassOf", starting from subject 10
    assert(GraphXTools.searchPropertySubGraph(triples, "http://www.w3.org/2000/01/rdf-schema#subClassOf", fromSubject = true, "http://purl.obolibrary.org/obo/NCBITaxon_10").count() == 2)
    //http://purl.obolibrary.org/obo/NCBITaxon_200
    //http://purl.obolibrary.org/obo/NCBITaxon_1

    //search descendants, ie subjects of property "subClassOf", starting from subject 10
    assert(GraphXTools.searchPropertySubGraph(triples, "http://www.w3.org/2000/01/rdf-schema#subClassOf", fromSubject = false, "http://purl.obolibrary.org/obo/NCBITaxon_10").count() == 5)
    //http://purl.obolibrary.org/obo/NCBITaxon_10000
    //http://purl.obolibrary.org/obo/NCBITaxon_200
    //http://purl.obolibrary.org/obo/NCBITaxon_1000
    //http://purl.obolibrary.org/obo/NCBITaxon_100
    //http://purl.obolibrary.org/obo/NCBITaxon_2000

    //search descendants of taxon 30 with respect of property rdfs:subPropertyOf
    assert(GraphXTools.searchPropertySubGraph(triples, "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", fromSubject = false, "http://purl.obolibrary.org/obo/NCBITaxon_30").count == 2)
    //http://purl.obolibrary.org/obo/NCBITaxon_300
    //http://purl.obolibrary.org/obo/NCBITaxon_400

    //look for a node that is not related to the property
    //assertThrows[NoSuchElementException](GraphXTools.searchPropertySubGraph(triples, "http://www.w3.org/2000/01/rdf-schema#subPropertyOf", fromSubject = false, "http://purl.obolibrary.org/obo/NCBITaxon_123456789"))

  }

  test("Get all descendants and ancestors of a taxon within a cyclic graph ") {
    testHelper.createTaxonGraph(withCycle = true)

    val wrapper = MsdWrapper(testHelper.generatedConfigFileName)
    val triples = wrapper.getTriples(testHelper.defaultMockGraphName, testHelper.millis)
    import net.sansa_stack.rdf.spark.model.GraphLoader
    val g = triples.asHashedGraph()
    g.triplets.count() //an action seems to required in test context. Strange.

    //say root is NCBITaxon_10, ignore superclass NCBITaxon_1
    val rootId = g.triplets.filter(t => t.toTuple._1._2.hasURI("http://purl.obolibrary.org/obo/NCBITaxon_10")).map(t => t.toTuple._1._1).take(1)(0)
    assert(GraphXTools.findNodeId(g, "http://purl.obolibrary.org/obo/NCBITaxon_10").get == rootId)
    assertThrows[NoSuchElementException](GraphXTools.findNodeId(g, "AbsentIRI").get)
    assert(GraphXTools.searchFromObject(g, rootId).count() == 8) //this result includes subClassOf AND fake subPropertyOf, not as in a property path

    val leafId = g.triplets.filter(t => t.toTuple._1._2.hasURI("http://purl.obolibrary.org/obo/NCBITaxon_10000")).map(t => t.toTuple._1._1).take(1)(0)

    //find parents of taxon 10
    assert(GraphXTools.searchFromSubject(g, rootId).count() == 2)
    //(0,http://purl.obolibrary.org/obo/NCBITaxon_1)
    //(1,http://purl.obolibrary.org/obo/NCBITaxon_200)


    //find ancestors of taxon 10000
    assert(GraphXTools.searchFromSubject(g, leafId).count() == 5)
    //(1,http://purl.obolibrary.org/obo/NCBITaxon_200)
    //(8,http://purl.obolibrary.org/obo/NCBITaxon_1000)
    //(2,http://purl.obolibrary.org/obo/NCBITaxon_1)
    //(72,http://purl.obolibrary.org/obo/NCBITaxon_100)
    //(40,http://purl.obolibrary.org/obo/NCBITaxon_10)

  }


  test("getGraphContent with filtered file list") {
    testHelper.create1MultifileMockGraphs2versions(testHelper.millis)

    val tools = MsdSparkTools(testHelper.generatedConfigFileName)

    val content = tools.getGraphContent(testHelper.defaultMockGraphName, testHelper.millis)

    assert(content.count() == 9)

    val fileList = dataLake.getGraph(testHelper.defaultMockGraphName, testHelper.millis).getFiles.filter(_.contains("testGraphFile1.nt"))
    val limitedContent = tools.getGraphContent(testHelper.defaultMockGraphName, testHelper.millis, fileList)

    assert(limitedContent.count() == 3)
  }

  test("CLI enhanceVoid with changing parameters (max ontology size and reasoner)") {
    testHelper.createMockDublinCore()
    testHelper.createMockCito()
    testHelper.create1MockGraphs2versions("1")
    testHelper.createMockEmptyChebiWithRealVoid()

    //Produce the standard void
    fr.inrae.msd.Main.main(Array("void", "-n", "testGraph", "--sparkStop", "false", "-c", testHelper.generatedConfigFileName))
    //Enhance it with ontologies less than 4000 triples, excluding chebi
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("enhanceVoid", "-n", "testGraph", "--maxOntologySize", "4000", "--sparkStop", "false", "-c", testHelper.generatedConfigFileName))
    }
    assert(!outCapture.toString().contains("chebi"))
    assert(outCapture.toString().contains("Using simple RDFS reasoner"))

    //Do it again with ontologies less than 1 000 000 000 triples, including chebi.
    //Should raise an exception because fake chebi has no content
    assertThrows[org.apache.hadoop.mapreduce.lib.input.InvalidInputException](fr.inrae.msd.Main.main(Array("enhanceVoid", "-n", "testGraph", "--maxOntologySize", "1000000000", "--sparkStop", "false", "-c", testHelper.generatedConfigFileName)))

    //Test again with Horst reasoner
    val outCapture2 = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture2) {
      fr.inrae.msd.Main.main(Array("enhanceVoid", "-n", "testGraph", "--useHorstReasoner", "true", "--sparkStop", "false", "-c", testHelper.generatedConfigFileName))
    }
    assert(outCapture2.toString().contains("Using Horst OWL reasoner"))

  }


  test("CLI void") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    fr.inrae.msd.Main.main(Array("void", "-n", "testGraph", "--sparkStop", "false", "-c", testHelper.generatedConfigFileName))
    val void = testHelper.cleanToCompare(dataLake.getGraphStats(testHelper.defaultMockGraphName, testHelper.millis))
    val graph = dataLake.getGraph(testHelper.defaultMockGraphName)
    val expected = testHelper.cleanToCompare(testHelper.getVoidContent(graph.graphDirPath))
    assert(void == expected)
  }

  test("CLI enhanceVoid") {
    testHelper.createMockCito()
    fr.inrae.msd.Main.main(Array("enhanceVoid", "-n", testHelper.mockCitoGraphName, "--sparkStop", "false", "-c", testHelper.generatedConfigFileName))
    val void = testHelper.cleanToCompare(dataLake.getGraphStats(testHelper.mockCitoGraphName, testHelper.millis))
    assert(void.contains("<http://stats.lod2.eu/rdf/void/?source=/data/spar_cito_v2.8.1><http://rdfs.org/ns/void#classes>\"9\""))
  }

  test("CLI void with non RDF graph") {
    testHelper.createNonRDFGraph()
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("void", "-n", "testGraph", "--sparkStop", "false", "-c", testHelper.generatedConfigFileName))
    }
    assert(outCapture.toString().contains("Graph testGraph format is not RDF or not recognised by Jena. Skipping VoID"))
  }

  test(s"Load cito and dublin core ontologies then enhance a graph with properties domains and ranges") {
    testHelper.create1MockGraphsUsingCitoAndDC(testHelper.millis)
    testHelper.createMockCito()
    testHelper.createMockDublinCore()

    //Add domains and ranges to a new VoID ntriple file
    MsdSparkTools(testHelper.generatedConfigFileName).enhanceVoid(testHelper.defaultMockGraphName, testHelper.millis, "")
    val void = Void(dataLake.getGraphStats(testHelper.defaultMockGraphName, ""))

    val ranges = void.model.listObjectsOfProperty(ResourceFactory.createProperty("http://inrae.fr/voidext/range")).toSet
    assert(ranges.size() == 2)
    val expectedRanges = List("http://purl.org/dc/terms/Agent", "http://www.w3.org/2001/XMLSchema#duration")
    ranges.forEach(r => assert(expectedRanges.contains(r.toString)))

    val domains = void.model.listObjectsOfProperty(ResourceFactory.createProperty("http://inrae.fr/voidext/domain")).toSet
    assert(domains.size() == 1)
    val expectedDomains = List("http://purl.org/spar/cito/Citation")
    domains.forEach(d => assert(expectedDomains.contains(d.toString)))

    assert(void.getRanges.size == 2)
    assert(void.getDomains.size == 1)
  }

  test(s"Api wrapper : get content of non RDF graph") {

    testHelper.createNonRDFGraph()
    val wrapper = MsdWrapper(testHelper.generatedConfigFileName)
    assertThrows[NoSuchFieldException](wrapper.getTriples(testHelper.defaultMockGraphName))

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      try {
        wrapper.getTriples(testHelper.defaultMockGraphName)
      }
      catch {
        case _: Throwable =>
      }
    }
    assert(outCapture.toString().contains("testGraph with format NON_RDF_FORMAT cannot be parsed by Jena. Skipping"))
  }

  test(s"Api wrapper test getSubGraphTriples with filtered file list") {
    testHelper.create1MultifileMockGraphs2versions(testHelper.millis)
    val wrapper = MsdWrapper(testHelper.generatedConfigFileName)
    assert(wrapper.getTriples(testHelper.defaultMockGraphName, testHelper.millis).count() == 9)
    val files = wrapper.dataLake.getGraph(testHelper.defaultMockGraphName).getFiles.filter(name => name.contains("File1") || name.contains("File2"))
    assert(files.size == 2)
    assert(wrapper.getSubGraphTriples(testHelper.defaultMockGraphName, testHelper.millis, files).count() == 6)

  }

  test(s"Api wrapper basic test") {
    testHelper.create1MultifileMockGraphs2versions(testHelper.millis)
    MsdSparkTools(testHelper.generatedConfigFileName).void(testHelper.defaultMockGraphName, testHelper.millis, "")
    val wrapper = MsdWrapper(testHelper.generatedConfigFileName)
    assert(wrapper.getTriples(testHelper.defaultMockGraphName, testHelper.millis).count() == 9)
    assert(wrapper.dataLake.getGraph(testHelper.defaultMockGraphName).getFilesUris.size == 3)
    assert(wrapper.dataLake.getGraph(testHelper.defaultMockGraphName).getFilesInputStreams.map(_.read() == 60).reduce(_ && _))
    assert(wrapper.dataLake.getGraphsWithSchemaItem(Vector(PropertyItem("predicate"))).size == 1)
  }


  test(s"SparkFunction Single VoID generation from a 1-file ntriple graph, threshold above number of triples") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val sparkFunctions = MsdSparkTools(testHelper.generatedConfigFileName)
    sparkFunctions.void(testHelper.defaultMockGraphName, testHelper.millis, "")
    val void = testHelper.cleanToCompare(dataLake.getGraphStats(testHelper.defaultMockGraphName, testHelper.millis))
    val graph = dataLake.getGraph(testHelper.defaultMockGraphName)
    val expected = testHelper.cleanToCompare(testHelper.getVoidContent(graph.graphDirPath))
    assert(void == expected)
  }


  test(s"SparkFunction Single VoID generation from a 1-file ntriple graph, threshold below number of triples") {

    testHelper.create1MockGraphs2versions(testHelper.millis)

    val sparkFunctions = MsdSparkTools(testHelper.generatedConfigFileName)
    sparkFunctions.void(testHelper.defaultMockGraphName, testHelper.millis, "")

    val void = testHelper.cleanToCompare(dataLake.getGraphStats(testHelper.defaultMockGraphName, testHelper.millis))
    val graph = dataLake.getGraph(testHelper.defaultMockGraphName)
    val expected = testHelper.cleanToCompare(testHelper.getVoidContent(graph.graphDirPath))

    assert(void == expected)

  }


  test(s"SparkFunction getGraphStat after single void generation") {

    testHelper.create1MockGraphs2versions(testHelper.millis)

    val sparkFunctions = MsdSparkTools(testHelper.generatedConfigFileName)
    sparkFunctions.void(testHelper.defaultMockGraphName, testHelper.millis, "")

    val void = testHelper.cleanToCompare(dataLake.getGraphStats(testHelper.defaultMockGraphName, testHelper.millis))
    val graph = dataLake.getGraph(testHelper.defaultMockGraphName)
    val expected = testHelper.cleanToCompare(testHelper.getVoidContent(graph.graphDirPath))

    assert(void == expected)

  }

  test(s"SparkFunction VoID generation then parsing by jena") {

    testHelper.createMockDublinCore()
    val sparkFunctions = MsdSparkTools(testHelper.generatedConfigFileName)
    sparkFunctions.void(testHelper.mockDCGraphName, testHelper.millis, "")

    val void = dataLake.getGraphStats(testHelper.mockDCGraphName, testHelper.millis)
    //val graph = testHelper.dataLake.getGraph(testHelper.defaultMockGraphName, "")
    //Rio.parse(new java.io.StringReader(void), "", RDFFormat.TURTLE)
    val model = ModelFactory.createDefaultModel
    RDFDataMgr.read(model, new java.io.StringReader(void), "", org.apache.jena.riot.Lang.TURTLE)
  }

  override def beforeEach(): Unit = {
    Try(testHelper.testFS.delete(new org.apache.hadoop.fs.Path(testHelper.testDir), true))
    testHelper.testFS.mkdirs(new org.apache.hadoop.fs.Path(testHelper.testDir))

  }

  override def afterEach(): Unit = {
    testHelper.testFS.delete(new org.apache.hadoop.fs.Path(testHelper.testDir), true)
  }

  private def getSession = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("sparFunctionsTest")
      .setMaster("local[*]")
      .set("spark.hadoop.fs.defaultFS", testHelper.cluster.getFileSystem().getUri.toString)
    SparkSession.builder.config(sparkConf).getOrCreate()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    getSession
  }

  override def afterAll(): Unit = {
    super.afterAll()
    org.apache.hadoop.fs.FileUtil.fullyDelete(new File(testHelper.generatedConfigFileName).getAbsoluteFile)
    //this.spark.stop()
    this.testHelper.cluster.shutdown()
  }


}