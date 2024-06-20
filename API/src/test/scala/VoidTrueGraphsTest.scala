import fr.inrae.msd.{ClassItem, DataLake, PropertyItem, TestHelper, Void}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.File
import scala.util.Try

class VoidTrueGraphsTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testHelper: TestHelper = TestHelper("./target/hdfs/")
  private val dataLake= new DataLake(testHelper.generatedConfigFileName, true)



  test("Test on true voids - look for properties #subPropertyOf and #seeAlso in datalake") {
    testHelper.createEmptyGraphsWithRealVoids()
    val errCapture = new java.io.ByteArrayOutputStream()
    Console.withErr(errCapture) { //Checking stdErr output
      val output = dataLake.getGraphsWithSchemaItem(List(PropertyItem("#subPropertyOf"), PropertyItem("#seeAlso"))).map(_.dirname)
      val expectedOutput =
        """dublin_core_dcmi_terms_v2020-01-20
          |metanetx_v4.4
          |sem_chem_v2.0
          |skos_v2011-08-06
          |spar_fabio_v2.2
          |""".stripMargin.split("\n")
      expectedOutput.foreach(g => assert(output.contains(g)))
      assert(expectedOutput.size == output.size)
    }

    val expectedError =
      """chebi_v2024-01-05
        |pubchem_compound_general_v2023-12-09
        |pubchem_descriptor_compound_general_v2023-12-09
        |pubchem_synonym_v2023-12-09
        |""".stripMargin.split("\n")

    expectedError.foreach(gn => assert(errCapture.toString().contains(gn)))

  }

  test("Test on true voids - look for property #subPropertyOf in datalake") {
    testHelper.createEmptyGraphsWithRealVoids()
    val errCapture = new java.io.ByteArrayOutputStream()
    Console.withErr(errCapture) { //Checking stdErr output
      val output = dataLake.getGraphsWithSchemaItem(List(PropertyItem("#subPropertyOf"))).map(_.dirname)
      val expectedOutput =
        """chebi_v2024-02-01
          |dublin_core_dcmi_terms_v2020-01-20
          |metanetx_v4.4
          |ncbitaxon_v2023-12-12
          |nlm_mesh_ontology_v0.9.3
          |sem_chem_v2.0
          |skos_v2011-08-06
          |spar_cito_v2.8.1
          |spar_fabio_v2.2""".stripMargin.split("\n")
      expectedOutput.foreach(g => assert(output.contains(g)))
      assert(expectedOutput.size == output.size)

    }

    val expectedError =
      """chebi_v2024-01-05
        |pubchem_compound_general_v2023-12-09
        |pubchem_descriptor_compound_general_v2023-12-09
        |pubchem_synonym_v2023-12-09
        |""".stripMargin.split("\n")

    expectedError.foreach(gn => assert(errCapture.toString().contains(gn)))

  }


  test("Test on true voids - look for class #Datatype and #DatatypeProperty in datalake") {
    testHelper.createEmptyGraphsWithRealVoids()
    val errCapture = new java.io.ByteArrayOutputStream()
    Console.withErr(errCapture) { //Checking stdErr output
      val output = dataLake.getGraphsWithSchemaItem(List(ClassItem("#Datatype"), ClassItem("#DatatypeProperty"))).map(_.dirname)
      val expectedOutput =
        """sem_chem_v2.0
          |spar_cito_v2.8.1
          |spar_fabio_v2.2""".stripMargin.split("\n")
      expectedOutput.foreach(g => assert(output.contains(g)))
    }


    val expectedError =
      """chebi_v2024-01-05
        |pubchem_compound_general_v2023-12-09
        |pubchem_descriptor_compound_general_v2023-12-09
        |pubchem_synonym_v2023-12-09
        |""".stripMargin.split("\n")

    expectedError.foreach(gn => assert(errCapture.toString().contains(gn)))

  }


  test("Test on true voids - look for class #Datatype in datalake") {
    testHelper.createEmptyGraphsWithRealVoids()
    val errCapture = new java.io.ByteArrayOutputStream()
    Console.withErr(errCapture) { //Checking stdErr output
      val output = dataLake.getGraphsWithSchemaItem(List(ClassItem("#Datatype"))).map(_.dirname)
      val expectedOutput =
        """dublin_core_dcmi_terms_v2020-01-20
          |sem_chem_v2.0
          |spar_cito_v2.8.1
          |spar_fabio_v2.2""".stripMargin.split("\n")
      expectedOutput.foreach(g => assert(output.contains(g)))
    }


    val expectedError =
      """chebi_v2024-01-05
        |pubchem_compound_general_v2023-12-09
        |pubchem_descriptor_compound_general_v2023-12-09
        |pubchem_synonym_v2023-12-09
        |""".stripMargin.split("\n")

    expectedError.foreach(gn => assert(errCapture.toString().contains(gn)))

  }



  override def beforeEach(): Unit = {
    Try(testHelper.testFS.delete(new org.apache.hadoop.fs.Path(testHelper.testDir), true))
    testHelper.testFS.mkdirs(new org.apache.hadoop.fs.Path(testHelper.testDir))
  }

  override def afterEach(): Unit = {
    testHelper.testFS.delete(new org.apache.hadoop.fs.Path(testHelper.testDir), true)
  }


  override def afterAll(): Unit = {
    super.afterAll()
    org.apache.hadoop.fs.FileUtil.fullyDelete(new File(testHelper.generatedConfigFileName).getAbsoluteFile)
    testHelper.cluster.shutdown()
  }

}