import fr.inrae.msd.{DataLake, TestHelper}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.File
import scala.util.Try
import sbtBuildInfo.BuildInfo

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

class CommandLineInterfaceTest2 extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testHelper: TestHelper = TestHelper("./target/hdfs/")
  val dl = new DataLake(testHelper.generatedConfigFileName,true)

  test("CLI with bad arg, should show usage") {
    val errCapture = new java.io.ByteArrayOutputStream()
    Console.withErr(errCapture) {
      fr.inrae.msd.Main.main(Array("toto","-c", testHelper.generatedConfigFileName))
    }
    assert (errCapture.toString().contains("Usage: msddbm [help|version|"))
  }

  test("CLI without config file") {
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array())
    }
    assert (outCapture.toString().contains("Config file msdQuery.conf doesn't exist. Stopping"))
  }

  test("get version") {

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("version", "-c", testHelper.generatedConfigFileName))
    }
    assert (outCapture.toString().contains(s"Version : ${BuildInfo.version}, with Scala version ${BuildInfo.scalaVersion}"))
  }

  test("get help") {

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("help", "-c", testHelper.generatedConfigFileName))
    }
    assert (outCapture.toString().contains("Usage: msddbm"))
  }

  test("get all ontologies and all instance graphs") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    testHelper.createMockCito()
    testHelper.createMockCito(latest = false, testHelper.millis + "_1")

    assert(dl.getAllInstanceGraphs.size == 2)
    assert(dl.getAllOntologies.size == 2)

  }

  test("List graphs with their sizes with chebi in scientific notation"){
    testHelper.create1MockGraphs2versions(testHelper.millis)
    testHelper.createMockCito()
    testHelper.createMockEmptyChebiWithRealVoid()

    fr.inrae.msd.Main.main(Array("gs", "-c", testHelper.generatedConfigFileName))


    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("gs", "-c", testHelper.generatedConfigFileName))
    }

    val chebiSize = 6860047.0
    val ontologiesTotal = 576.0 + chebiSize
    val instanceGraphsTotal = 0.0
    val globalTotal = ontologiesTotal + instanceGraphsTotal


    val got = outCapture.toString()

    assert(got.contains(f"mock_cito ${testHelper.millis}, TURTLE, 576 triples, ontology"))
    assert(got.contains(s"testGraph ${testHelper.millis}, NT, instance graph"))
    assert(got.contains(f"mock_chebi ${testHelper.millis}, RDFXML, ${chebiSize}%.2e triples, ontology"))
    assert(got.contains("Number of graphs listed: 3"))
    assert(got.contains(f"Latest ontologies total: ${ontologiesTotal}%.2e triples"))
    assert(got.contains(f"Latest instance graphs total: ${instanceGraphsTotal}%.2e triples"))
    assert(got.contains(f"Global: ${globalTotal}%.2e triples"))


  }

//  test("List graphs with their sizes retrieved from their VoID"){
//    testHelper.create1MockGraphs2versions(testHelper.millis)
//    testHelper.createMockCito()
//
//    val outCapture = new java.io.ByteArrayOutputStream()
//    Console.withOut(outCapture) {
//      fr.inrae.msd.Main.main(Array("gs", "-c", testHelper.generatedConfigFileName))
//    }
//
//    val ontologiesTotal = 576.0
//    val instanceGraphsTotal = 0.0
//    val globalTotal = ontologiesTotal + instanceGraphsTotal
//
//    val expected = testHelper.cleanToCompare( f"""mock_cito ${testHelper.millis}, TURTLE, 576 triples, ontology
//                                                 |testGraph ${testHelper.millis}, NT, instance graph
//                                                 |
//                                                 |Number of graphs listed : 2
//                                                 |
//                                                 |Latest ontologies total: ${ontologiesTotal}%.2e triples
//                                                 |Latest instance graphs total:${instanceGraphsTotal}%.2e triples
//                                                 |Global : ${globalTotal}%.2e triples
//                                                 |""".stripMargin)
//
//    val got = testHelper.cleanToCompare(outCapture.toString())
//
//    assert(got == expected)
//
//  }


  test("List instance graphs only"){
    testHelper.create1MockGraphs2versions(testHelper.millis)
    testHelper.createMockCito()

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("lsi", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare( s"""testGraph ${testHelper.millis}, NT
                                                 |Number of graphs listed : 1
                                                 |""".stripMargin)

    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got == expected)

  }


  test("List ontologies only"){
    testHelper.create1MockGraphs2versions(testHelper.millis)
    testHelper.createMockCito()

    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("lso", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare( s"""mock_cito ${testHelper.millis}, TURTLE
                                                 |Number of graphs listed : 1
                                                 |""".stripMargin)

    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got == expected)

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