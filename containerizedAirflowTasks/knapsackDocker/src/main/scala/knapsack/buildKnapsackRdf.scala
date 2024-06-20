package knapsack

import com.m3.curly.scala._
import org.eclipse.rdf4j.model.util.ModelBuilder
import org.eclipse.rdf4j.model.vocabulary.{RDF, RDFS}
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.util.Try


object buildKnapsackRdf extends App {

  org.apache.log4j.BasicConfigurator.configure()

  private val HADOOP_USER_NAME = scala.util.Properties.envOrElse("HADOOP_USER_NAME", "hadoop")
  private val JSON_INPUT_PATH = scala.util.Properties.envOrElse("JSON_INPUT_PATH", "/data/tmp/knapsack.json")
  private val TTL_OUTPUT_FILE_NAME = scala.util.Properties.envOrElse("TTL_OUTPUT_FILE_NAME", "knapsack.ttl")
  private val VERSION_OUTPUT_FILE_NAME = scala.util.Properties.envOrElse("VERSION_OUTPUT_FILE_NAME", "knapsack.version")
  private val HDFS_TMP_DIR = scala.util.Properties.envOrElse("HDFS_TMP_DIR", "/data/tmp/")
  private val HADOOP_URL = scala.util.Properties.envOrElse("HADOOP_URL", "http://localhost:9870")
  private val HADOOP_PWD = scala.util.Properties.envOrElse("HADOOP_PWD", "****")

  val userPwd = HADOOP_USER_NAME + ":" + HADOOP_PWD
  val creditentials = Base64.getEncoder.encodeToString(userPwd.getBytes(StandardCharsets.UTF_8))


  val requete = Request(HADOOP_URL
    + "/webhdfs/v1/"
    + JSON_INPUT_PATH
    + "?user.name="
    + HADOOP_USER_NAME
    + "&op=OPEN")
    .header("Authorization", "Basic " + creditentials)

  val hdfsTextFile: String = HTTP.get(requete).asString
  val data: ujson.Value = ujson.read(hdfsTextFile)

  val urlBase = "http://www.knapsackfamily.com/knapsack_core/"


  /* "C_ID", "CAS ID", "Metabolite", "Molecular formula", "Mw", "Organism" */
  val header = data(0)("children").arr.map(x => x("text").str)

  val metabolites =
    data.arr.drop(1).map(x => (
      (urlBase + x.obj("children").arr(0)("children").arr(0)("href").str),
      Try(x.obj("children").arr(0)("children").arr(0)("text").str).getOrElse(""),
      Try(x.obj("children").arr(1)("text").str).getOrElse(""),
      Try(x.obj("children").arr(2)("text").str).getOrElse(""),
      Try(x.obj("children").arr(3)("text").str).getOrElse(""),
      Try(x.obj("children").arr(4)("text").str).getOrElse(""),
      Try(x.obj("children").arr(5)("text").str).getOrElse("")
    )).toSeq

  val builder: ModelBuilder = new ModelBuilder()

  builder.setNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
  builder.setNamespace("chebi", "http://purl.obolibrary.org/obo/chebi#")
  builder.setNamespace("oboInOwl", "http://www.geneontology.org/formats/oboInOwl#")
  builder.setNamespace("metabohub", "http://www.metabohub.org/semantics/resource/2022#")

  // rdfs:label "C0000"
  // rdfs:label "Metabolite"
  // oboInOwl:hasExactSynonym "Metabolite"
  // chebi:formula "C29HXX..."
  // chebi:mass "33."
  // oboInOwl:hasDbXref CAS:2941-20-0

  metabolites.foreach {
    case (uri, id, casId, name, molecular, mw, organism) => {
      builder.subject(uri)
        .add(RDF.TYPE, "metabohub:Compound")
        .add(RDF.TYPE, "metabohub:KnapsackCompound")
        .add(RDFS.LABEL, name)
        .add(RDFS.LABEL, id)
        .add("oboInOwl:hasExactSynonym", name)
        .add("chebi:formula", molecular)
        .add("chebi:mass", mw)
        .add("metabohub:taxon", organism)
        .add("oboInOwl:hasDbXref", s"CAS:$casId")
    }
  }

  val model = builder.build()
  val version = "N" + model.subjects().size()
  writeHdfs(HDFS_TMP_DIR,VERSION_OUTPUT_FILE_NAME,version)

  val stringWriter = new StringWriter()
  Rio.write(model, stringWriter, RDFFormat.TURTLE)
  writeHdfs(HDFS_TMP_DIR,TTL_OUTPUT_FILE_NAME,stringWriter.toString)

  //Effaçage du json d'origine :
  val requeteDeleteJson = Request(HADOOP_URL
    + "/webhdfs/v1"
    + JSON_INPUT_PATH
    + "?op=DELETE")
    .header("Authorization", "Basic " + creditentials)
  Try(HTTP.delete(requeteDeleteJson))




  def writeHdfs(dir: String, fileName: String, content: String): Unit = {

    val requeteMkDir = Request(HADOOP_URL
      + dir
      + "?op=MKDIR")
      .header("Authorization", "Basic " + creditentials)
    //Création répertoire temporaire
    Try(HTTP.put(requeteMkDir))


    val requeteDelete = Request(HADOOP_URL
      + "/webhdfs/v1"
      + dir
      + fileName
      + "?op=DELETE")
      .header("Authorization", "Basic " + creditentials)
    //Effacage du fichier si restant d'une MAJ ratée, etc.
    Try(HTTP.delete(requeteDelete))



    val requetePutRes1 = Request(HADOOP_URL
      + "/webhdfs/v1"
      + dir
      + fileName
      + "?user.name="
      + HADOOP_USER_NAME
      + "&op=CREATE&overwrite=true")
      .followRedirects(false)
      .header("Authorization", "Basic " + creditentials)

    val rep = HTTP.put(requetePutRes1)

    if (rep.status() != 307)
      throw new Exception("307 attendu à la demande de création partie 1, obtenu: " + rep.status())

    val redirRequest = rep
      .headerFields()
      .getOrElse("Location", throw new Exception("Datanode non présent dans la réponse ? :/"))
      .mkString

    val requetePutRes2 = Request(redirRequest)
      .header("Authorization", "Basic " + creditentials)
      .body(content.getBytes())

    HTTP.put(requetePutRes2)
  }



}