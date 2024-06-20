import fr.inrae.msd.{DataLake, KnowledgeGraph, TestHelper}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.util.{Failure, Try}

class CommandLineInterfaceTest extends AnyFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testHelper: TestHelper = TestHelper("./target/hdfs/")
  val dl = new DataLake(testHelper.generatedConfigFileName,true)


  test("Look for inexistent graph") {
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("listFilesNames", "-n", "testGraph2", "-c", testHelper.generatedConfigFileName))
    }
    val expected = testHelper.cleanToCompare("Couldn't retrieve graph testGraph2  : empty.headError getting graph testGraph2  (empty.head)\nError")
    assert(testHelper.cleanToCompare(outCapture.toString()) == expected)
  }

  test(s"main  listFileNames") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("listFilesNames", "-n", "testGraph", "-c", testHelper.generatedConfigFileName))
    }

    val expected = "testGraph.nt"
    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got == expected)
  }

  test(s"main  listFilePaths ") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("listFilesPaths", "-n", "testGraph", "-c", testHelper.generatedConfigFileName))
    }

    val expectedEnd = "/testGraph.nt"
    val expectedBeginning = "/"
    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got.endsWith(expectedEnd) && got.startsWith(expectedBeginning))
  }

  test(s"main  listFilesUris") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("listFilesUris", "-n", "testGraph", "-c", testHelper.generatedConfigFileName))
    }

    val expectedEnd = "/testGraph.nt"
    val expectedBeginning = "hdfs://"
    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got.endsWith(expectedEnd) && got.startsWith(expectedBeginning))
  }

  test(s"main  listAllGraphs ") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("listAllGraphs", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare(s"testGraph ${testHelper.millis} ,NT testGraph ${testHelper.millis}_1, NT" + "Number of graphs listed : 2")
    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got == expected)
  }

  test(s"main listGraphs") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("listGraphs", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare(s"testGraph ${testHelper.millis}, NT" + "Number of graphs listed : 1")
    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got == expected)
  }

  test(s"main metadata -n testGraph") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("metadata", "-n", "testGraph", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare(s"""{"download_url":"https://www.example.com/","version":"${testHelper.millis}","actual_file_list":["testGraph.nt"],"dir_name":"testGraph","compression":"","format":"NT","date_importation_msd":"2023-11-15T13:19:21.575546","ontology_namespace":""}""")
    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got == expected)
  }

  test(s"main metadata -n testGraph -v ${testHelper.millis}_1") {
    testHelper.create1MockGraphs2versions(testHelper.millis)
    val outCapture = new java.io.ByteArrayOutputStream()
    Console.withOut(outCapture) {
      fr.inrae.msd.Main.main(Array("metadata", "-n", "testGraph", "-v", s"${testHelper.millis}_1", "-c", testHelper.generatedConfigFileName))
    }

    val expected = testHelper.cleanToCompare(s"""{"download_url":"https://www.example.com/","version":"${testHelper.millis}_1","actual_file_list":["testGraph.nt"],"dir_name":"testGraph","compression":"","format":"NT","date_importation_msd":"2023-11-15T13:19:21.575546","ontology_namespace":""}""")
    val got = testHelper.cleanToCompare(outCapture.toString())

    assert(got == expected)
  }

  test("DataLake.getAllGraphs: is a given version present (2 available)") {

    testHelper.create1MockGraphs2versions(testHelper.millis)

    val all: Vector[KnowledgeGraph] =
      dl.getAllGraphs
    assert(all.length == 2)
    val testGraph = all
      .filter(_.dirname.equals("testGraph"))
      .filter(_.graphVersion.equals(testHelper.millis))

    assert(testGraph.length == 1)
    assert(testGraph.head.graphVersion.equals(testHelper.millis))
  }

  test("DataLake.getLatestGraphs: get latest version on 2 present ") {

    testHelper.create1MockGraphs2versions(testHelper.millis)

    val everyLatest: Vector[KnowledgeGraph] = dl.getLatestGraphs
    assert(everyLatest.length == 1)
    val testGraph = everyLatest
      .filter(_.dirname.equals("testGraph"))

    assert(testGraph.length == 1)
    assert(testGraph.head.graphVersion.equals(testHelper.millis))
  }

  test("DataLake.getLatestGraphs: get latest version on 2 present with JSON metadata corrupt ") {

    testHelper.create1MockGraphs2versions1IsBroken(testHelper.millis)

    val t = Try {
      val everyLatest: Vector[KnowledgeGraph] = dl.getLatestGraphs
      assert(everyLatest.length == 1)
      val testGraph = everyLatest
        .filter(_.dirname.equals("testGraph"))

      assert(testGraph.length == 1)
      assert(testGraph.head.graphVersion.equals(testHelper.millis))
    }

    assert(t.isFailure)

    t match {
      case Failure(exception) => assert (exception.getMessage.contains("Failed to read JSON from HDFS while parsing hdfs://"))
    }

  }

  test("DataLake.getLatestGraphs: request on an empty lake") {

    assert(dl.getAllGraphs.isEmpty)

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