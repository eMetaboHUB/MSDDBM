import fr.inrae.msd.{ClassItem, DataLake, PropertyItem, TestHelper, Void}
import org.apache.jena.rdf.model.{RDFNode, Resource}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.util.Try
import org.apache.jena.vocabulary._

class VoidTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testHelper: TestHelper = TestHelper("./target/hdfs/")
  private val dataLake = new DataLake(testHelper.generatedConfigFileName,true)


  test(s"Main.main  findClass -l #ObjectProperty,#Class  -c  ${testHelper.generatedConfigFileName} Check error output with broken void file ") {
    testHelper.createMockCitoWithBrokenVoid()
    println("This test should print an error.\".")

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withErr(outCapture) { //Checking stdErr output
      fr.inrae.msd.Main.main(Array("findClass", "-l", "#ObjectProperty,#Class", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare(s"Error reading model :")

    val got = testHelper.cleanToCompare(outCapture.toString())
    assert(got.contains(expected))
  }

  test(s"Main.main  findClass -l #ObjectProperty,#Class  -c  ${testHelper.generatedConfigFileName} with broken void file ") {
    testHelper.createMockCitoWithBrokenVoid()

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("findClass", "-l", "#ObjectProperty,#Class", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare("0 graph containing following items: #ObjectProperty #Class")

    val got = testHelper.cleanToCompare(outCapture.toString())
    assert(got == expected)
  }

  test(s"Main.main  findClass -l #ObjectProperty,#Class  -c  ${testHelper.generatedConfigFileName} with no void file present") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("findClass", "-l", "#ObjectProperty,#Class", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare("0 graph containing following items: #ObjectProperty #Class")

    val got = testHelper.cleanToCompare(outCapture.toString())
    assert(got == expected)
  }

  test(s"Main.main  findClass -l #ObjectProperty,#Class  -c  ${testHelper.generatedConfigFileName}") {
    testHelper.createMockCito()

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("findClass", "-l", "#ObjectProperty,#Class", "-c", testHelper.generatedConfigFileName))
    }

    val expectedHeader = testHelper.cleanToCompare(
      s"""
         1 graph containing following items: #ObjectProperty #Class
         |mock_cito ${testHelper.millis} (located in /data/APITestDir/mock_cito_v${testHelper.millis})""".stripMargin)

    val class1 = testHelper.cleanToCompare("Class http://www.w3.org/2002/07/owl#ObjectProperty found 98 time(s)")
    val class2 = testHelper.cleanToCompare("Class http://www.w3.org/2002/07/owl#Class found 10 time(s)")

    val cleanedCapture = testHelper.cleanToCompare(outCapture.toString())
    assert(cleanedCapture.contains(expectedHeader))
    assert(cleanedCapture.contains(class1))
    assert(cleanedCapture.contains(class2))

  }

  test(s"Main.main  findProp -l #inverseOf,#first with 2 graphs in the result") {

    testHelper.createMockCito()
    testHelper.createMockCito(dir="cito2")
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("findProp", "-l", "#inverseOf,#first", "-c", testHelper.generatedConfigFileName))
    }


    val expectedHeader1 = testHelper.cleanToCompare(
      s"""2 graphs containing following items:
         |#inverseOf #first
      """.stripMargin)

    val expectedHeader2 = testHelper.cleanToCompare(
      s"""cito2 ${testHelper.millis} (located in /data/APITestDir/cito2_v${testHelper.millis})
         |Property http://www.w3.org/2002/07/owl#inverseOf found 43 time(s)
         |Property http://www.w3.org/1999/02/22-rdf-syntax-ns#first found 4 time(s)
      """.stripMargin)

    val expectedHeader3 = testHelper.cleanToCompare(
      s"""mock_cito ${testHelper.millis} (located in /data/APITestDir/mock_cito_v${testHelper.millis})
         |Property http://www.w3.org/2002/07/owl#inverseOf found 43 time(s)
         |Property http://www.w3.org/1999/02/22-rdf-syntax-ns#first found 4 time(s)
      """.stripMargin)

    val p1 = testHelper.cleanToCompare("Property http://www.w3.org/1999/02/22-rdf-syntax-ns#first found 4 time(s)")
    val p2 = testHelper.cleanToCompare("Property http://www.w3.org/2002/07/owl#inverseOf found 43 time(s)")

    val cleanedCapture = testHelper.cleanToCompare(outCapture.toString())
    assert(cleanedCapture.contains(expectedHeader1))
    assert(cleanedCapture.contains(expectedHeader2))
    assert(cleanedCapture.contains(expectedHeader3))
    assert(cleanedCapture.contains(p1))
    assert(cleanedCapture.contains(p2))
  }

  test(s"Main.main  findProp -l #inverseOf,#first with an old version of a graph") {

    testHelper.createMockCito()
    testHelper.createMockCito(latest = false,testHelper.millis+"_1")
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("findProp", "-l", "#inverseOf,#first", "-c", testHelper.generatedConfigFileName))
    }


    val expectedHeader = testHelper.cleanToCompare(
      s"""
         1 graph containing following items: #inverseOf #first
         |mock_cito ${testHelper.millis} (located in /data/APITestDir/mock_cito_v${testHelper.millis})""".stripMargin)

    val p1 = testHelper.cleanToCompare("Property http://www.w3.org/1999/02/22-rdf-syntax-ns#first found 4 time(s)")
    val p2 = testHelper.cleanToCompare("Property http://www.w3.org/2002/07/owl#inverseOf found 43 time(s)")

    val cleanedCapture = testHelper.cleanToCompare(outCapture.toString())
    assert(cleanedCapture.contains(expectedHeader))
    assert(cleanedCapture.contains(p1))
    assert(cleanedCapture.contains(p2))
  }

  test(s"Main.main  findProp -l #inverseOf,#first  -c  ${testHelper.generatedConfigFileName}") {

    testHelper.createMockCito()
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("findProp", "-l", "#inverseOf,#first", "-c", testHelper.generatedConfigFileName))
    }


    val expectedHeader = testHelper.cleanToCompare(
      s"""
         1 graph containing following items: #inverseOf #first
         |mock_cito ${testHelper.millis} (located in /data/APITestDir/mock_cito_v${testHelper.millis})""".stripMargin)

    val p1 = testHelper.cleanToCompare("Property http://www.w3.org/1999/02/22-rdf-syntax-ns#first found 4 time(s)")
    val p2 = testHelper.cleanToCompare("Property http://www.w3.org/2002/07/owl#inverseOf found 43 time(s)")

    val cleanedCapture = testHelper.cleanToCompare(outCapture.toString())
    assert(cleanedCapture.contains(expectedHeader))
    assert(cleanedCapture.contains(p1))
    assert(cleanedCapture.contains(p2))
  }


  test("get VoID numbers") {
    testHelper.createMockDublinCore()
    val void = Void(dataLake.getGraphStats(testHelper.mockDCGraphName, ""))

    assert(void.getEntities == 155)
    assert(void.getDistinctObjects == 76)
    assert(void.getDistinctSubjects == 99)
  }

  test("dataLake.getGraphsWithProperties : retrieve DC") {

    testHelper.createMockDublinCore()
    val someProps = Vector(PropertyItem("http://purl.org/dc/terms/issued"), PropertyItem("http://www.w3.org/2000/01/rdf-schema#isDefinedBy"))
    assert(dataLake.getGraphsWithSchemaItem(someProps).size == 1)

    val someOtherProps = Vector(PropertyItem("nawak"), PropertyItem("http://www.w3.org/2000/01/rdf-schema#isDefinedBy"))
    assert(dataLake.getGraphsWithSchemaItem(someOtherProps).isEmpty)

    val someOtherOtherProps = Vector(PropertyItem(""), PropertyItem("http://www.w3.org/2000/01/rdf-schema#isDefinedBy"))
    assert(dataLake.getGraphsWithSchemaItem(someOtherOtherProps).size == 1)
  }

  test("dataLake.getGraphsWithProperties : retrieve cito") {

    testHelper.createMockCito()
    val someProps = Vector(PropertyItem("http://www.w3.org/2002/07/owl#inverseOf"), PropertyItem("http://www.w3.org/1999/02/22-rdf-syntax-ns#first"))
    assert(dataLake.getGraphsWithSchemaItem(someProps).size == 1)
    val someShortProps = Vector(PropertyItem("inverseOf"), PropertyItem("first"))
    assert(dataLake.getGraphsWithSchemaItem(someShortProps).size == 1)

    val someOtherProps = Vector(PropertyItem("nawak"), PropertyItem("http://www.w3.org/1999/02/22-rdf-syntax-ns#first"))
    assert(dataLake.getGraphsWithSchemaItem(someOtherProps).isEmpty)

  }

  test("dataLake.getGraphsWithProperties : retrieve cito AND DC") {

    testHelper.createMockCito()
    testHelper.createMockDublinCore()
    val someProps = Vector(PropertyItem("subPropertyOf"))
    assert(dataLake.getGraphsWithSchemaItem(someProps).size == 2)


  }

  test(s"Main.main  stats with no void") {
    testHelper.create1MultifileMockGraphs2versions(testHelper.millis)

    val outCapture = new java.io.ByteArrayOutputStream()

    Console.withOut(outCapture) { //msddbm stats -n dublin_core_terms,
      fr.inrae.msd.Main.main(Array("stats", "-n", testHelper.defaultMockGraphName, "-c", testHelper.generatedConfigFileName))
    }

    val expected = ""
    val got = testHelper.cleanToCompare(outCapture.toString())
    assert(got == expected)
  }

  test(s"Main.main  stats -n ${testHelper.mockDCGraphName}  -c  ${testHelper.generatedConfigFileName} : assert void is retrieved") {
    testHelper.createMockDublinCore()
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) { //msddbm stats -n dublin_core_terms,
      fr.inrae.msd.Main.main(Array("stats", "-n", testHelper.mockDCGraphName, "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare(testHelper.getDcVoidString)
    val got = testHelper.cleanToCompare(outCapture.toString())
    assert(got == expected)
  }


  test("Void class tools") {

    testHelper.createMockDublinCore()

    val void = Void(dataLake.getGraphStats(testHelper.mockDCGraphName, ""))


    assert(void.getClassPartition.size == 5)
    assert(void.getClassPartition.map(i=>i._1.asResource().getURI->i._2).getOrElse("http://purl.org/dc/dcam/VocabularyEncodingScheme",0) == 9)

    assert(void.hasItem(ClassItem("http://purl.org/dc/dcam/VocabularyEncodingScheme")))
    assert(!void.hasItem(ClassItem("http://purl.org/dc/dcam/VocabularyEncodingSchem")))

    val someClasses = Vector(ClassItem("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"), ClassItem("http://purl.org/dc/terms/AgentClass"))
    assert(void.hasMultipleItems(someClasses))

    val someNonExistentClasses = Vector(ClassItem("http://nawak"), ClassItem("http://purl.org/dc/terms/AgentClass"))
    assert(!void.hasMultipleItems(someNonExistentClasses))

    val someShortClasses = Vector(ClassItem("Property"), ClassItem("AgentClass"))
    assert(void.hasMultipleItemSubstrings(someShortClasses))

    val someNonExistentShortClasses = Vector(ClassItem("nawak"), ClassItem("AgentClass"))
    assert(!void.hasMultipleItemSubstrings(someNonExistentShortClasses))

    assert(void.hasItemSubstring(ClassItem("http://purl.org/dc/dcam/VocabularyEncodingScheme")))
    assert(void.hasItemSubstring(ClassItem("VocabularyEncodingScheme")))

  }

  test("Void Property tools") {

    testHelper.createMockDublinCore()

    val void = Void(dataLake.getGraphStats(testHelper.mockDCGraphName, ""))


    assert(void.getPropertyPartition.size == 17)
    assert(void.getPropertyPartition.getOrElse(DCTerms.issued, 0) == 98) //"http://purl.org/dc/terms/issued",0) == 98)


    assert(void.hasItem(PropertyItem(DCTerms.issued.getURI))) // "http://purl.org/dc/terms/issued")))
    assert(!void.hasItem(PropertyItem("http://purl.org/dc/terms/issue")))

    val someProps = Vector(PropertyItem(DCTerms.issued.getURI), PropertyItem(RDFS.isDefinedBy.getURI)) // "http://www.w3.org/2000/01/rdf-schema#isDefinedBy"))
    assert(void.hasMultipleItems(someProps))

    val someNonExistentProps = Vector(PropertyItem("http://nawak"), PropertyItem("http://purl.org/dc/terms/issued"))
    assert(!void.hasMultipleItems(someNonExistentProps))

    val someShortProps = Vector(PropertyItem("issued"), PropertyItem("isDefinedBy"))
    assert(void.hasMultipleItemSubstrings(someShortProps))

    val someNonExistentShortProps = Vector(PropertyItem("nawak"), PropertyItem("isDefinedBy"))
    assert(!void.hasMultipleItemSubstrings(someNonExistentShortProps))

    assert(void.hasItemSubstring(PropertyItem("http://www.w3.org/2000/01/rdf-schema#isDefinedBy")))
    assert(void.hasItemSubstring(PropertyItem("isDefinedBy")))

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