

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.jena.sparql.engine.binding.Binding
import net.sansa_stack.query.spark.sparqlify._
import org.apache.spark.sql.{Encoder, Encoders}
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.rdf.spark.model.TripleOperations


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

val wrapper = MsdWrapper(spark)

/* Provide the Jena triplets associated with the knowledge graphs accessible on the datalake */
val graphs = wrapper.dataLake.getLatestGraphs


def taxonQuery2(fabioProperty: String) = s"""
PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
PREFIX fabio: <http://purl.org/spar/fabio/>
PREFIX cito: <http://purl.org/spar/cito/> 
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX ncbiTaxon: <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?pmid ?pmidTitle ?compound {
  # Find the broader class for taxon 3700 (Brassicaceae)
  ncbiTaxon:NCBITaxon_3700 rdfs:subClassOf ?broaderTaxon .
  
  # Find subclasses of broaderTaxon, 
  ?cousins rdfs:subClassOf ?broaderTaxon .

  # Find cousin's labels
  ?cousins rdfs:label ?cousinLabel .

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

val taxonTriples2 : RDD[Triple] = Seq("ncbitaxon","nlm_mesh","nlm_mesh_ontology","pubchem_reference","pubchem_substance").map {  
   db =>  wrapper.getTriples(db)
}.reduce( (t1,t2) => t1.union(t2))

taxonTriples2.toDS().coalesce(nExecutors)

val resultBindings1: RDD[Binding] = execQuery(taxonQuery2("fabio:hasSubjectTerm"),taxonTriples2)
val resultBindings2: RDD[Binding] = execQuery(taxonQuery2("fabio:hasPrimarySubjectTerm"),taxonTriples2)
val resultBindings = resultBindings1.union(resultBindings2)

implicit val ssEnco: Encoder[(String,String,String)] = Encoders.kryo(classOf[(String,String,String)])


val pmids_compounds = resultBindings.map( bindings => (bindings.get("pmid"),bindings.get("pmidTitle"),bindings.get("compound")) ).collect()
pmids_compounds.sortBy(_._3.toString()).foreach(println)
pmids_compounds.sortBy(_._3.toString()).map(_.toString()).foreach(log)



