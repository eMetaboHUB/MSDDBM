package fr.inrae.msd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.slf4j.impl.StaticLoggerBinder

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source

/**This class is a mess, not to be used outside tests TODO: tidying
 * Yet it belongs to the API as it it useful for BraChemDB
 * The client class shall have an already created "src/main/resources/" directory
 * It generates a test environment with a mini HDFS cluster and a datalake, with the ability to create mock knowledge graphs
 * @see [[generatedConfigFileName]] config file name that will generated for the tests according to the mini cluster config.
 *
 */
case class TestHelper(baseDirName : String) {

  val baseDir: File = new File(baseDirName).getAbsoluteFile
  val generatedConfigFileName = "src/main/resources/gen-msdQueryTest.conf"
  val testDir: String = "/data/APITestDir"
  val defaultMockGraphName = "testGraph"
  val mockDCGraphName = "mock_dublin_core"
  val mockCitoGraphName = "mock_cito"
  val mockChebiGraphName = "mock_chebi"
  val rdfSyntaxDir = "rdf_syntax"

  val conf: Configuration = setConf(baseDir)

  val cluster: MiniDFSCluster = setupMiniDatalake()
  createConfigFile(cluster, testDir, generatedConfigFileName)
  val testFS: org.apache.hadoop.fs.FileSystem = cluster.getFileSystem()
  //DataLake(testFS, generatedConfigFileName)
  val millis: String = System.currentTimeMillis().toString



  val taxonGraphNtWithCycle: String =
    """
      |<http://purl.obolibrary.org/obo/NCBITaxon_10> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_1> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_100> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_10> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_100> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_10000> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_1000> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_200> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_10> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_2000> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_200> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_10> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_200> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_10000000> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://purl.obolibrary.org/obo/ncbitaxon#has_rank> <http://purl.obolibrary.org/obo/NCBITaxon_species> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.geneontology.org/formats/oboInOwl#hasDbXref> "GC_ID:11" .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.geneontology.org/formats/oboInOwl#hasOBONamespace> "ncbi_taxonomy" .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.w3.org/2000/01/rdf-schema#label> "Pseudomonas sp. 5K-VPa" .
      |<http://purl.obolibrary.org/obo/NCBITaxon_30> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://purl.obolibrary.org/obo/NCBITaxon_10> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_300> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://purl.obolibrary.org/obo/NCBITaxon_30> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_400> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://purl.obolibrary.org/obo/NCBITaxon_30> .
      |""".stripMargin

  val taxonGraphNt: String =
    """
      |<http://purl.obolibrary.org/obo/NCBITaxon_10> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_1> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_100> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_10> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_100> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_10000> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_1000> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_200> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_10> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_2000> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_200> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_10000000> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://purl.obolibrary.org/obo/ncbitaxon#has_rank> <http://purl.obolibrary.org/obo/NCBITaxon_species> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.geneontology.org/formats/oboInOwl#hasDbXref> "GC_ID:11" .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.geneontology.org/formats/oboInOwl#hasOBONamespace> "ncbi_taxonomy" .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1000009> <http://www.w3.org/2000/01/rdf-schema#label> "Pseudomonas sp. 5K-VPa" .
      |<http://purl.obolibrary.org/obo/NCBITaxon_30> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://purl.obolibrary.org/obo/NCBITaxon_10> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_300> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://purl.obolibrary.org/obo/NCBITaxon_30> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_400> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://purl.obolibrary.org/obo/NCBITaxon_30> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_3700> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_3699> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_947472> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_3700> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_3699> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_91836> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_91836> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_71275> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_71275> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_1437201> .
      |<http://purl.obolibrary.org/obo/NCBITaxon_1437201> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/NCBITaxon_91827> .
      |
      |
      |
      |""".stripMargin


  val rdfXmlGraph: String =
    """<http://example.org/subject1> <http://example.org/predicate> "Object 1" .
      |<http://example.org/subject2> <http://example.org/predicate> "Object 2" .
      |<http://example.org/subject3> <http://example.org/predicate> "Object 3" .""".stripMargin

  val rdfXmlGraphUsingCitoAndDC: String =
    """<http://example.org/subject1> <http://purl.org/spar/cito/confirms> "Object 1 confirming subject1" .
      |<http://example.org/citation> <http://purl.org/spar/cito/hasCitationTimeSpan> "P5Y" .
      |<http://example.org/thisGraph> <http://purl.org/dc/terms/creator> "Me" .""".stripMargin


  def createTaxonGraph(withCycle: Boolean = false) : Unit = {
    if (withCycle)
      createMockGraph(version=millis,latest=true,content = taxonGraphNtWithCycle)
    else
      createMockGraph(version=millis,latest=true,content = taxonGraphNt)
  }

  def createRealNcbiTaxon(): Unit = {
    val dir = defaultMockGraphName
    val version = millis
    createTestMetaFile(version,latest = true,fileList = "\"ncbitaxon.nt\"",dir,ontologyUri = "http://purl.obolibrary.org/obo/")
    val testGraphDirString = testDir + "/" + dir + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
    testFS.mkdirs(testGraphDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/ncbitaxon.nt").getPath),testGraphDir)
  }

  def create1MockGraphsUsingCitoAndDC(version: String): Unit = {
    createMockGraph(version = version, latest = true,content = rdfXmlGraphUsingCitoAndDC)
    val testGraphDirString = testDir + "/" + defaultMockGraphName + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
    val voidDir = new org.apache.hadoop.fs.Path(testGraphDir + "_analysis/void/")
    testFS.mkdirs(testGraphDir)
    testFS.mkdirs(voidDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/mockVoidWithCitoAndDcProperties.ttl").getPath),voidDir)

  }

  def create1MockGraphs2versions1IsBroken(version: String): Unit = {
    createMockGraph(version = version, latest = true,broken = true) //same file, labeled latest
    createMockGraph(version = version + "_1", latest = false) // another (older) version
  }

  def create1MockGraphs2versions(version: String): Unit = {
    createMockGraph(version = version, latest = true) //same file, labeled latest
    createMockGraph(version = version + "_1", latest = false) // another (older) version
  }

  def create1MultifileMockGraphs2versions(n: String): Unit = {
    //assert(DataLake.getAllGraphs.isEmpty)
    val fileList = Vector("testGraphFile1.nt","testGraphFile2.nt", "testGraphFile3.nt" )
    createMockGraph(fileNameList=fileList, version = n, latest = true) //same file, labeled latest
    createMockGraph(fileNameList=fileList, version = n + "_1", latest = false) // another (older) version
  }

  def createTestMetaFile(version: String, latest: Boolean, fileList: String = "\"testGraph.nt\"", dirName: String = defaultMockGraphName, format : String = "NT", ontologyUri:String="", broken: Boolean = false): Path = {

    if (format == "RDFXML") {
      println("WARNING ! miniDFSCluster/Sansa/Jena doesn't support RDFXML very well. Please use other RDF format for tests.")
    }
    val nameEnd = if (latest) {
      "latest"
    } else {
      "v" + version
    }

    val testMetaFilePath = new org.apache.hadoop.fs.Path(testDir + "/" + dirName + "_metadata_" + nameEnd)
    val testMetaFile = testFS.create(testMetaFilePath)
    val testFileContent = if(!broken) {
      s"""{"download_url": "https://www.example.com/",
         |"version":  "$version",
         |"actual_file_list": [$fileList],
         |"dir_name": "$dirName",
         |"compression": "",
         |"format": "$format",
         |"date_importation_msd": "2023-11-15T13:19:21.575546",
         |"ontology_namespace": "$ontologyUri"
         |}""".stripMargin
    } else {
      s"""{"download_url": "https://www.example.com/",
         |"version
         |}""".stripMargin

    }


    testMetaFile.write(testFileContent.getBytes())
    testMetaFile.close()
    testMetaFilePath
  }

  def createNonRDFGraph(): Unit = {
    createMockGraph(version="0",latest=true,format = "NON_RDF_FORMAT")
  }

  def createMockGraph(fileNameList: Vector[String] = Vector("testGraph.nt"),
                      dir: String = defaultMockGraphName,
                      version: String,
                      latest: Boolean,
                      content : String = rdfXmlGraph,
                     format: String = "NT", broken: Boolean = false): Unit = {

    createTestMetaFile(version, latest, fileNameList.map(fileName => "\"" + fileName + "\"").reduce(_+","+_), dirName=dir,format=format, broken = broken)
    val testGraphDir = new org.apache.hadoop.fs.Path(testDir + "/" + dir + "_v" + version + "/")
    testFS.mkdirs(testGraphDir)
    fileNameList.foreach(fileName => {
      val testGraphPath = new org.apache.hadoop.fs.Path(testGraphDir, fileName)
      val testGraphFile = testFS.create(testGraphPath)
      testGraphFile.write(content.getBytes())
      testGraphFile.close()
    }
    )
  }

  def writeFileToHdfs(path: String, fileName: String, content: String ): Unit = {
    val p = new org.apache.hadoop.fs.Path(path)
    testFS.mkdirs(p)
    val filePath = new org.apache.hadoop.fs.Path(path, fileName)
    val file = testFS.create(filePath)
    file.write(content.getBytes())
    file.close()
  }


  def createRdfSyntaxOntology(): Unit = {
    val dir = rdfSyntaxDir
    val version = millis
    createTestMetaFile(version,latest = true,fileList = "\"22-rdf-syntax-ns.nt\"",dir,ontologyUri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    val testGraphDirString = testDir + "/" + dir + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
//    val voidDir = new org.apache.hadoop.fs.Path(testGraphDir + "_analysis/void/")
    testFS.mkdirs(testGraphDir)
//    testFS.mkdirs(voidDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/ontologies/22-rdf-syntax-ns.nt").getPath),testGraphDir)
//    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/dcVoidOutput.ttl").getPath),voidDir)
  }

  def createMockDublinCore(): Unit = {
    val dir = mockDCGraphName
    val version = millis
    createTestMetaFile(version,latest = true,fileList = "\"dublin_core_terms.nt\"",dir,ontologyUri = "http://purl.org/dc/terms/")
    val testGraphDirString = testDir + "/" + dir + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
    val voidDir = new org.apache.hadoop.fs.Path(testGraphDir + "_analysis/void/")
    testFS.mkdirs(testGraphDir)
    testFS.mkdirs(voidDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/dublin_core_terms.nt").getPath),testGraphDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/dcVoidOutput.ttl").getPath),voidDir)
  }

  def createMockCito(latest: Boolean = true, version : String = millis, dir : String = mockCitoGraphName): Unit = {

    createTestMetaFile(version,latest,fileList = "\"cito.ttl\"",dir,format = "TURTLE",ontologyUri = "http://purl.org/spar/cito/")
    val testGraphDirString = testDir + "/" + dir + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
    val voidDir = new org.apache.hadoop.fs.Path(testGraphDir + "_analysis/void/")
    testFS.mkdirs(testGraphDir)
    testFS.mkdirs(voidDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/cito.ttl").getPath),testGraphDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/cito-void.ttl").getPath),voidDir)
  }

  def createMockEmptyChebiWithRealVoid(latest: Boolean = true, version : String = millis, dir : String = mockChebiGraphName): Unit = {

    createTestMetaFile(version,latest,fileList = "\"chebi.owl\"",dir,format = "RDFXML",ontologyUri = "http://purl.obolibrary.org/obo/chebi/")
    val testGraphDirString = testDir + "/" + dir + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
    val voidDir = new org.apache.hadoop.fs.Path(testGraphDir + "_analysis/void/")
    testFS.mkdirs(testGraphDir)
    testFS.mkdirs(voidDir)
    //testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/cito.ttl").getPath),testGraphDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/voidsForTests/__data__chebi_v2024-02-01_analysis__void__part-00000").getPath),voidDir)
  }

  def createMockCitoWithBrokenVoid(): Unit = {
    val dir = mockCitoGraphName
    val version = millis
    createTestMetaFile(version,latest = true,fileList = "\"cito.ttl\"",dir,format = "TURTLE")
    val testGraphDirString = testDir + "/" + dir + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
    val voidDir = new org.apache.hadoop.fs.Path(testGraphDir + "_analysis/void/")
    testFS.mkdirs(testGraphDir)
    testFS.mkdirs(voidDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/cito.ttl").getPath),testGraphDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/cito-voidWithSyntaxError.ttl").getPath),voidDir)
  }

  def createMockEmptyWithRealVoid(voidName : String): Unit = {
    val dirname = voidName.replace("_analysis__void__part-00000","").replace("__data__","")
    val version = millis
    createTestMetaFile(version,latest = true,fileList = "\"nonExistent.ttl\"",dirname,format = "TURTLE")
    val testGraphDirString = testDir + "/" + dirname + "_v" + version + "/"
    val testGraphDir = new org.apache.hadoop.fs.Path(testGraphDirString)
    val voidDir = new org.apache.hadoop.fs.Path(testGraphDir + "_analysis/void/")
    //testFS.mkdirs(testGraphDir)
    testFS.mkdirs(voidDir)
    testFS.copyFromLocalFile(new org.apache.hadoop.fs.Path(getClass.getResource("/voidsForTests/" + voidName).getPath),voidDir)
  }

  def createEmptyGraphsWithRealVoids():Unit = {
    val lsOutput = """__data__chebi_v2024-01-05_analysis__void__part-00000
                     |__data__chebi_v2024-02-01_analysis__void__part-00000
                     |__data__chembl_v33.0_analysis__void__part-00000
                     |__data__dublin_core_dcmi_terms_v2020-01-20_analysis__void__part-00000
                     |__data__knapsack_scrap_vN63723_analysis__void__part-00000
                     |__data__metanetx_v4.4_analysis__void__part-00000
                     |__data__ncbitaxon_v2023-12-12_analysis__void__part-00000
                     |__data__nlm_mesh_ontology_v0.9.3_analysis__void__part-00000
                     |__data__nlm_mesh_vSHA_4bc58657_analysis__void__part-00000
                     |__data__pubchem_compound_general_v2023-12-09_analysis__void__part-00000
                     |__data__pubchem_compound_general_v2024-02-03_analysis__void__part-00000
                     |__data__pubchem_descriptor_compound_general_v2023-12-09_analysis__void__part-00000
                     |__data__pubchem_descriptor_compound_general_v2024-02-03_analysis__void__part-00000
                     |__data__pubchem_inchikey_v2023-12-09_analysis__void__part-00000
                     |__data__pubchem_inchikey_v2024-02-03_analysis__void__part-00000
                     |__data__pubchem_reference_v2024-02-03_analysis__void__part-00000
                     |__data__pubchem_substance_v2024-02-03_analysis__void__part-00000
                     |__data__pubchem_synonym_v2023-12-09_analysis__void__part-00000
                     |__data__pubchem_synonym_v2024-02-03_analysis__void__part-00000
                     |__data__pubchem_void_v2023-12-09_analysis__void__part-00000
                     |__data__pubchem_void_v2024-02-03_analysis__void__part-00000
                     |__data__sem_chem_v2.0_analysis__void__part-00000
                     |__data__skos_v2011-08-06_analysis__void__part-00000
                     |__data__spar_cito_v2.8.1_analysis__void__part-00000
                     |__data__spar_fabio_v2.2_analysis__void__part-00000
                     |""".stripMargin
    val voids = lsOutput.split("\n")
    voids.foreach(createMockEmptyWithRealVoid)
  }

  def getDcVoidString: String = {
    val voidPath = getClass.getResource("/dcVoidOutput.ttl").getPath
    val source = Source.fromFile(voidPath)
    val fileContents = source.getLines.mkString
    source.close()
    fileContents
  }

  private def createConfigFile(cluster : MiniDFSCluster, dataDir: String, outputFile: String): String = {
    val configFileContent: String =
      s"""
         |hadoop {
         |CONF_DIR: "/fake/hadoop"
         |USER_NAME: "fakehadoop"
         |HOME_DIR: "/fake/opt/hadoop"
         |URL:"${cluster.getURI}"
         |}
         |datalake {
         |DATA_DIR: "$dataDir"
         |LOG_FILE: "/data/msddbm.log"
         |}
         |""".stripMargin

    Files.write(Paths.get(outputFile), configFileContent.getBytes(StandardCharsets.UTF_8))
    configFileContent
  }

  private def setConf(bDir: File): Configuration = {
    // Configure SLF4J to use the NOP logger during tests, as miniDFSCluster is very noisy
    val nopLoggerBinder = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    //if (!nopLoggerBinder.contains("nop"))
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "off")

    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, bDir.getAbsolutePath)

    conf
  }

  private def setupMiniDatalake(): MiniDFSCluster = {
    val builder = new MiniDFSCluster.Builder(conf)
    builder.build()
  }

  def cleanToCompare(s: String): String = s.replace("\n", "").replace(" ", "")


  def getVoidContent(filename : String): String ={
    s"""
       |@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
       |                    @prefix void: <http://rdfs.org/ns/void#> .
       |                    @prefix void-ext: <http://stats.lod2.eu/rdf/void-ext/> .
       |                    @prefix qb: <http://purl.org/linked-data/cube#> .
       |                    @prefix dcterms: <http://purl.org/dc/terms/> .
       |                    @prefix ls-void: <http://stats.lod2.eu/rdf/void/> .
       |                    @prefix ls-qb: <http://stats.lod2.eu/rdf/qb/> .
       |                    @prefix ls-cr: <http://stats.lod2.eu/rdf/qb/criteria/> .
       |                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
       |                    @prefix xstats: <http://example.org/XStats#> .
       |                    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
       |                    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
       |<http://stats.lod2.eu/rdf/void/?source=$filename>
       |
       |void:classes 0;
       |void:entities  4;
       |void:distinctSubjects  3;
       |void:distinctObjects  0;
       |void:properties 1;
       |void:propertyPartition [
       |void:property <http://example.org/predicate>;
       |void:triples 3;
       |];
       |void:triples 3;
       |void:vocabulary  <http://example.org/>;
       |a void:Dataset .
       |""".stripMargin
  }


}