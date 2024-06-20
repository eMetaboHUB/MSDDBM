

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.jena.sparql.engine.binding.Binding
import net.sansa_stack.query.spark.sparqlify._
import org.apache.spark.sql.{Encoder, Encoders}
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.rdf.spark.model.TripleOperations
import spark.implicits._



import org.apache.jena.graph.Triple
import org.apache.spark.sql.Dataset

import fr.inrae.msd._

val nExecutors=40

def execQuery(queryS:String, triples:RDD[Triple]) : RDD[Binding] = {
    new QueryEngineFactorySparqlify(spark)
        .create(triples)
        .createQueryExecution(queryS)
        .execSelectSpark()
        .getBindings
}

def log(s: String): Unit = {
    import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
    import scala.util.Try

    val fileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val logFileName="/data/braChemDb.log"

    println(s)

    val hdfsPath = new Path(logFileName)

    def getStream: FSDataOutputStream = {
      if (fileSystem.exists(hdfsPath))
        fileSystem.append(hdfsPath)
      else
        fileSystem.create(hdfsPath)
    }

    Try(getStream).map(
      stream => Try({
      stream.writeBytes(s + "\n")
      stream.close()
    }))
}

log("Starting new search for Brassicales' compounds")

val wrapper = MsdWrapper(spark)

/* Provide the Jena triplets associated with the knowledge graphs accessible on the datalake */
val graphs = wrapper.dataLake.getLatestGraphs


//BEGIN cheating : emulate Pregel output graph with 4 taxa to test the request
// import org.apache.jena.graph.{Node, NodeFactory, Triple}

// def nodeToTriple(nodeUri: String): Triple = {
//     val node = NodeFactory.createURI(nodeUri)
//     val pred = NodeFactory.createURI("http://inrae.fr/propertySubGraphElementOf")
//     val obj = NodeFactory.createURI("http://purl.obolibrary.org/obo/NCBITaxon_3699")
//     new Triple(node, pred, obj)
// }
// val taxaStrings = Array("http://purl.obolibrary.org/obo/NCBITaxon_301453", "http://purl.obolibrary.org/obo/NCBITaxon_4018", "http://purl.obolibrary.org/obo/NCBITaxon_26958","http://purl.obolibrary.org/obo/NCBITaxon_4324")
// val taxaTriples = taxaStrings.map(nodeToTriple)
// val taxaSubGraphElements = sc.parallelize(taxaTriples)
//END CHEATING

//1: find ancestor of http://purl.obolibrary.org/obo/NCBITaxon_3700 (Brassicaceae)
val taxonTriples : RDD[Triple] =  wrapper.getTriples("ncbitaxon")
taxonTriples.toDS().coalesce(nExecutors)

val tool = GraphXTools()
//search ancestors, ie objects of property "subClassOf", starting from subject NCBITaxon_3700
val superTaxon = tool.searchPropertySubGraph(taxonTriples,"http://www.w3.org/2000/01/rdf-schema#subClassOf",fromSubject = true, "http://purl.obolibrary.org/obo/NCBITaxon_3700",1 )
val superTaxonUri = superTaxon.take(1).map(_.getURI).apply(0)
//BEGIN CHEATING
//val superTaxonUri = "http://purl.obolibrary.org/obo/NCBITaxon_3699" //here we cheat and start from 1 level above Brassicaceae to gain some time
//END CHEATING

//search descendants, ie subjects of property "subClassOf", starting from subject "superTaxon""
val taxaSubGraphElements = tool.searchPropertySubGraphAsGraph(
    taxonTriples,
    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
    fromSubject = false,
    superTaxonUri ,
    maxIterations = 15 )



def articlesQuery(fabioProperty: String) = s"""
PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
PREFIX fabio: <http://purl.org/spar/fabio/>
PREFIX cito: <http://purl.org/spar/cito/> 
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX ncbiTaxon: <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

# Select distinct values for PubMed ID, PubMed Title, and Compound
SELECT DISTINCT ?pmid ?pmidTitle ?compound {

  ?cousinId <http://inrae.fr/propertySubGraphElementOf> ?parent .

  # Find cousin's labels
  ?cousinId rdfs:label ?cousinLabel .

  # Find cousin's MeSH descriptor. TODO: search terms, pref. terms etc
  ?d rdfs:label ?cousinLabel .
  ?d a meshv:TopicalDescriptor .
  
  # Connect PubMed articles to MeSH descriptors
  # using the FABIO property specified in the data
  ?pmid $fabioProperty ?d .
  
  # Get the title of each PubMed article
  ?pmid dcterms:title ?pmidTitle .
  
  # Connect substances to articles using CITO ontology
  ?substance cito:isDiscussedBy ?pmid .
  
  # Connect substances to compounds using the SIO ontology
  ?substance sio:CHEMINF_000477 ?compound .
}
"""


val refTriples : RDD[Triple] = Seq("ncbitaxon","nlm_mesh","nlm_mesh_ontology","pubchem_reference","pubchem_substance").map {
   db =>  wrapper.getTriples(db)
}.reduce( (t1,t2) => t1.union(t2))

val articleTriples : RDD[Triple] = refTriples.union(taxaSubGraphElements)

articleTriples.toDS().coalesce(nExecutors)



val resultBindings1: RDD[Binding] = execQuery(articlesQuery("fabio:hasSubjectTerm"),articleTriples)
val resultBindings2: RDD[Binding] = execQuery(articlesQuery("fabio:hasPrimarySubjectTerm"),articleTriples)
val resultBindings = resultBindings1.union(resultBindings2)
implicit val ssEnco: Encoder[(String,String,String)] = Encoders.kryo(classOf[(String,String,String)])

val pmids_compounds = resultBindings.map( binding => (binding.get("pmid"),binding.get("pmidTitle"),binding.get("compound")) ).collect()

pmids_compounds.sortBy(_._3.toString()).map(_.toString()).foreach(log)

log("Ending new search for Brassicales' compounds")


