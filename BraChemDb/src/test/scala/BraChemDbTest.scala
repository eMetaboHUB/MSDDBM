package fr.inrae.brachemdb

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import fr.inrae.msd.TestHelper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

class BraChemDbTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val t: TestHelper = TestHelper("./target/hdfs/")
  val testFS: FileSystem = t.testFS



  test("Test dummy result generation / workflow integration "){
    val outputDir = t.testDir
    val flagFileName = "results"
    val flagPath = new Path(outputDir + "/" + flagFileName)
    val resultCopyName = new Path(flagPath.toString + "-complete")

    val f = Follower()
    val e = Explorer

    f.makeDiff(outputDir,"brachemdbOutput-",flagFileName)
    assert(testFS.exists(flagPath))
    assert(!testFS.exists(resultCopyName))
    assert(f.checkForNewResults(t.testDir, "brachemdbOutput-",t.testDir+"/diffLog")._1.isEmpty)


    createGraphs()
    e.runExploration(outputDir,t.generatedConfigFileName,testContext = true,1,manualPartition = false)

    Thread.sleep(1000)
    f.makeDiff(outputDir, "brachemdbOutput-",flagFileName)
    assert(testFS.exists(flagPath))
    assert(testFS.exists(resultCopyName))



    e.fakeRun1(outputDir)
    assert(f.checkForNewResults(t.testDir, "brachemdbOutput-",t.testDir+"/diffLog")._1.size == 3)
    f.makeDiff(outputDir, "brachemdbOutput-",flagFileName)
    assert(testFS.exists(flagPath))
    assert(testFS.exists(resultCopyName))

    val diffContent: String = f.readFile(flagPath.toString)
    assert(diffContent.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID398553>"))
    assert(diffContent.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID10494>"))
    assert(diffContent.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713>"))

    val resContent = f.readFile(resultCopyName.toString)
    assert(resContent.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID398553>"))
    assert(resContent.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID10494>"))
    assert(resContent.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713>"))

    e.fakeRun2(outputDir)
    assert(f.checkForNewResults(t.testDir, "brachemdbOutput-",t.testDir+"/diffLog")._1.size == 1)
    f.makeDiff(outputDir, "brachemdbOutput-",flagFileName)
    assert(testFS.exists(flagPath))
    assert(testFS.exists(resultCopyName))

    val resContent2 = f.readFile(resultCopyName.toString)
    assert(resContent2.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID398553>"))
    assert(resContent2.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID10494>"))
    assert(resContent2.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID2713>"))
    assert(resContent2.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID8082>"))


    val diffContent2: String = f.readFile(flagPath.toString)
    assert(diffContent2.contains("<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID8082>"))


  }

  test("Test boilerplate functions") {
    val f = Follower()
    val liste: String = """hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716875255957
                          |hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716884614734
                          |hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716888767913
                          |hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716892390413
                          |hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716896127550
                          |hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716899109207
                          |hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716901360159
                          |hdfs://147.100.175.223:9000/data/braChemDb/brachemdbOutput-1716901360159.log""".stripMargin

    val res = """1716875255957
                |1716884614734
                |1716888767913
                |1716892390413
                |1716896127550
                |1716899109207
                |1716901360159
                |1716901360159.log""".stripMargin.split("\n")

    liste.split("\n").map(s=>f.getEndOfString(s,"/data/braChemDb/brachemdbOutput-")).foreach(s=> assert(res.contains(s)))
    assert(f.getEndOfString("hdfs://147.100.175.223:9000/data/brachemdb/brachemdbOutput-1715874181852", "/data/brachemdb/brachemdbOutput-") == "1715874181852")
  }






  def createGraphs(): Unit = {
    t.createMockGraph(dir = "ncbitaxon", version = "1", latest = true, content = taxonContent)
    t.createMockGraph(dir = "nlm_mesh", version = "1", latest = true, content = meshContent)
    t.createMockGraph(dir = "nlm_mesh_ontology", version = "1", latest = true, content = meshOntContent)
    t.createMockGraph(dir = "pubchem_reference", version = "1", latest = true, content = refContent)
    t.createMockGraph(dir = "pubchem_substance", version = "1", latest = true, content = substContent)
  }


  val taxonContent: String =
    """
      |<http://purl.obolibrary.org/obo/NCBITaxon_4324> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_3699> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_3700> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_3699> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_980082> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_3700> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_359864> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_980082> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_3700> <http://www.w3.org/2000/01/rdf-schema#label> "Brassicaceae"^^<http://www.w3.org/2001/XMLSchema#string> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_980082> <http://www.w3.org/2000/01/rdf-schema#label> "Aethionemeae"^^<http://www.w3.org/2001/XMLSchema#string> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_359864> <http://www.w3.org/2000/01/rdf-schema#label> "Moriera"^^<http://www.w3.org/2001/XMLSchema#string> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_4324> <http://www.w3.org/2000/01/rdf-schema#label> "Salvadoraceae"^^<http://www.w3.org/2001/XMLSchema#string> .
      |""".stripMargin

  val meshContent: String =
    """
      |<http://id.nlm.nih.gov/mesh/D000001> <http://www.w3.org/2000/01/rdf-schema#label> "Brassicaceae"^^<http://www.w3.org/2001/XMLSchema#string> .
      |<http://id.nlm.nih.gov/mesh/D000002> <http://www.w3.org/2000/01/rdf-schema#label> "Aethionemeae"^^<http://www.w3.org/2001/XMLSchema#string> .
      |<http://id.nlm.nih.gov/mesh/D000003> <http://www.w3.org/2000/01/rdf-schema#label> "Moriera"^^<http://www.w3.org/2001/XMLSchema#string> .
      |<http://id.nlm.nih.gov/mesh/D032121> <http://www.w3.org/2000/01/rdf-schema#label> "Salvadoraceae"^^<http://www.w3.org/2001/XMLSchema#string> .
      |<http://id.nlm.nih.gov/mesh/D000001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor> .
      |<http://id.nlm.nih.gov/mesh/D000002> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor> .
      |<http://id.nlm.nih.gov/mesh/D000003> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor> .
      |<http://id.nlm.nih.gov/mesh/D032121> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor> .
      |""".stripMargin

  //We don't really use MeSH voc
  val meshOntContent: String =
    """
      |<http://id.nlm.nih.gov/mesh/vocab#TopicalDescriptor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Class> .
      |""".stripMargin

  val refContent: String =
    """
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/reference/23488078> <http://purl.org/spar/fabio/hasSubjectTerm> <http://id.nlm.nih.gov/mesh/D000002> .
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/reference/23488078> <http://purl.org/spar/fabio/hasPrimarySubjectTerm> <http://id.nlm.nih.gov/mesh/D000001> .
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/reference/23488078> <http://purl.org/dc/terms/title> "In Planta Protein Sialylation..."^^<http://www.w3.org/2001/XMLSchema#string> .
      |""".stripMargin

  val substContent: String =
    """
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/substance/SID464338003> <http://purl.org/spar/cito/isDiscussedBy> <http://rdf.ncbi.nlm.nih.gov/pubchem/reference/23488078> .
      |<http://rdf.ncbi.nlm.nih.gov/pubchem/substance/SID464338003> <http://semanticscience.org/resource/CHEMINF_000477> <http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID1005> .
      |""".stripMargin


  override def beforeEach(): Unit = {
    Try(testFS.delete(new org.apache.hadoop.fs.Path(t.testDir), true))
    testFS.mkdirs(new org.apache.hadoop.fs.Path(t.testDir))

  }

  override def afterEach(): Unit = {
    testFS.delete(new org.apache.hadoop.fs.Path(t.testDir), true)
  }

  override def beforeAll(): Unit = {super.beforeAll()
     val sparkConf: SparkConf = new SparkConf()
      .setAppName("brachemdbTest")
      .setMaster("local[2]")
      .set("spark.hadoop.fs.defaultFS", t.cluster.getFileSystem().getUri.toString)


     SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "1g")
      .config(sparkConf).getOrCreate()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    t.cluster.shutdown()
  }


}