package fr.inrae.msd


import org.apache.jena.rdf.model.{ModelFactory, Property, RDFNode, ResourceFactory}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary._

import java.util
import scala.Console.err
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.util.Try


/** Models a VoID stats file
 *
 * @param modelString a VoID RDF file in TTL format
 */
case class Void(modelString: String) {

  val model: org.apache.jena.rdf.model.Model = getModelFromTurtleOrNTString(modelString)

  /** Looks for the presence of a property or class in a graph
   *
   * @param item a class/property name or substring of it's name that will be looked for in the VoID
   * @return true if the class/property was found
   */
  def hasItem(item: GraphSchemaItem): Boolean = {
    item match {
      case _: PropertyItem => getPropertyPartition.keySet.map(_.toString).contains(item.name)
      case _: ClassItem => getClassPartition.keySet.map(_.toString).contains(item.name)
      case _ => (getPropertyPartition ++ getClassPartition).keySet.map(_.toString).contains(item.name)
    }
  }

  /** Looks for the presence of a list of classes or properties in a graph
   *
   * @param names a list of exact class/property names that will be looked for in the VoID
   * @return true if the all the classes/properties were found
   */
  def hasMultipleItems(names: Seq[GraphSchemaItem]): Boolean = {
    Try(names.map(hasItem).reduce(_ && _)).getOrElse(false)
  }

  /** Looks for the presence of a list of substrings of classes or properties in a graph
   *
   * @param names a list of class/property names or substrings of their names that will be looked for in the VoID
   * @return true if all the classes/properties were found
   */
  def hasMultipleItemSubstrings(names: Seq[GraphSchemaItem]): Boolean = {
    Try(names.map(hasItemSubstring).reduce(_ && _)).getOrElse(false)
  }

  /** Looks for the presence of a substring of a class or property in a graph
   *
   * @param item a graph item containing a substring of a class or property name that will be looked for in the VoID
   * @return true if a class containing the substring was found
   */

  def hasItemSubstring(item: GraphSchemaItem): Boolean = {
    def checkPresence(partition: Map[RDFNode, Long], itm: GraphSchemaItem): Boolean = {
      partition.keySet.foldLeft(false)((accumulated, key) => accumulated || key.toString.endsWith(itm.name))
    }

    item match {
      case _: PropertyItem => checkPresence(getPropertyPartition, item)
      case _: ClassItem => checkPresence(getClassPartition, item)
      case _ => checkPresence(getPropertyPartition ++ getClassPartition, item)
    }
  }

  /** Gets the number of distinct URIs in the graph
   *
   * @return the number of entities in the graph
   */
  def getEntities: Long = Try(model.listObjectsOfProperty(VOID.entities).map(_.asLiteral.getLong ).next()).getOrElse(0)

  /** Gets the number of distinct URIs in the graph subjects
   *
   * @return the number of subjects in the graph
   */
  def getDistinctSubjects: Long = Try(model.listObjectsOfProperty(VOID.distinctSubjects).map(_.asLiteral.getLong).next()).getOrElse(0)

  /** Gets the number of distinct URIs in the graph objects
   *
   * @return the number of objects in the graph
   */
  def getDistinctObjects: Long = Try(model.listObjectsOfProperty(VOID.distinctObjects).map(_.asLiteral.getLong).next()).getOrElse(0)

  /**Retrieves the total graph size from its VoID
   * @return Graph size, expressed in triples.
   */
  def getGraphSize: Long = {
    val statementIterator = model.listStatements().filter(_ != null) //Java in Scala :/
      .filter(_.getSubject != null)
      .filter(_.getSubject.getURI != null)
      .filter(_.getPredicate != null)
      .filter(s=> s.getSubject.getURI.contains("http://stats.lod2.eu/rdf/void/?source=") &&  s.getPredicate == VOID.triples)

    Try(statementIterator.next().getObject.asLiteral.getLong).getOrElse(0)
  }

  /** Gets the classes of the graph, and their number of occurrences
   *
   * @return a Map containing the classes associated with their number of occurrences
   */
  def getClassPartition: Map[RDFNode, Long] = {
    getPartition(VOID.classPartition, VOID._class)
  }

  /** Gets the properties of the graph, and their number of occurrences
   *
   * @return a Map containing the properties associated with their number of occurrences
   */
  def getPropertyPartition: Map[RDFNode, Long] = {
    getPartition(VOID.propertyPartition, VOID.property)
  }

  /** Gets either a list of classes or a list of properties from a VoID Model
   *
   * @param partitionType either "http://rdfs.org/ns/void#propertyPartition" or "http://rdfs.org/ns/void#classPartition"
   * @param objectType    either "http://rdfs.org/ns/void#property" or "http://rdfs.org/ns/void#class"
   * @return a Map containing the properties or classes associated with their number of occurrences
   */
  private def getPartition(partitionType: Property, objectType: Property): Map[RDFNode, Long] = {
    val triplesProperty = VOID.triples

    model.listObjectsOfProperty(partitionType)
      .map(_.asResource)
      .flatMap(item => Map(model.getProperty(item, objectType).getObject -> model.getProperty(item, triplesProperty).getLong))
      .toMap
  }

  /**Gets ranges of each property from an enhanced VoID property partition
   * @return a Map of properties associated Sets of ranges
   */
  def getRanges: Map[RDFNode, util.Set[RDFNode]] = {
      getRangesOrDomains("range")
  }

  /**Gets domains of each property from an enhanced VoID property partition
   * @return a Map of properties associated Sets of domains
   */
  def getDomains: Map[RDFNode, util.Set[RDFNode]] = {
    getRangesOrDomains("domain")
  }


  /**Gets ranges or domains of each property from an enhanced VoID property partition
   * @param pType Either "ranges" or "domains"
   * @return a Map of properties associated to either a Set of "ranges" or "domains"
   */
  private def getRangesOrDomains(pType : String) = {
    //"http://rdfs.org/ns/void#range doesn't exist, creating : http://inrae.fr/voidext/range and http://inrae.fr/voidext/domain"
    model.listObjectsOfProperty(VOID.propertyPartition)
      .map(_.asResource)
      .filter(item =>Try( model.getProperty(item, ResourceFactory.createProperty ("http://inrae.fr/voidext/" + pType)).getObject.toString).getOrElse("").nonEmpty)
      .flatMap(item =>
        Map(model.getProperty(item, VOID.property).getObject
          -> model.listObjectsOfProperty(ResourceFactory.createProperty ("http://inrae.fr/voidext/" + pType)).toSet) )
      .toMap
  }


  /**Converts a NTRIPLES or TURTLE VoID file into a Jena Model for a given graph
   * @param voidGraph the VoID graph as written by distLODStats
   * @return the Jena Model of the VoID graph
   */
  private def getModelFromTurtleOrNTString(voidGraph: String): org.apache.jena.rdf.model.Model = {
    Try(getModelFromString(voidGraph, org.apache.jena.riot.Lang.TURTLE)).getOrElse(
      getModelFromString(voidGraph, org.apache.jena.riot.Lang.NTRIPLES)
    )
  }

  /**Converts VoID file into a Jena Model for a given graph
   * @param voidGraph the VoID graph as written by distLODStats
   * @param lang the VoID serialization type
   * @return the Jena Model of the VoID graph, or an empty model if parsing failed
   */
  private def getModelFromString(voidGraph: String, lang: org.apache.jena.riot.Lang): org.apache.jena.rdf.model.Model = {
    //Jena init fails when run from a fat jar without these 2 lines
    new org.apache.jena.sys.InitJenaCore().start()
    new org.apache.jena.riot.system.InitRIOT().start()

    val model = ModelFactory.createDefaultModel
    val res = Try(RDFDataMgr.read(model, new java.io.StringReader(voidGraph), "", lang))
    if (res.isFailure)
      err.println(s"Error reading model : " + voidGraph)

    model
  }

}


/** Helps functions determine the kind of schema element they are given or asked for
 *
 */
abstract class GraphSchemaItem {
  val name: String

  override def toString: String = name //simply prints the name instead of class(name)
}

/** Schema item representing a graph property
 *
 * @param name URI or substring of a property URI
 */
case class PropertyItem(name: String) extends GraphSchemaItem

/** Schema item representing a graph class
 *
 * @param name URI or substring of a class URI
 */
case class ClassItem(name: String) extends GraphSchemaItem

/*
val props =
  """
   http://id.nlm.nih.gov/mesh/vocab#abbreviation
    |http://id.nlm.nih.gov/mesh/vocab#active
    |http://id.nlm.nih.gov/mesh/vocab#allowableQualifier
    |http://id.nlm.nih.gov/mesh/vocab#altLabel
    |http://id.nlm.nih.gov/mesh/vocab#annotation
    |http://id.nlm.nih.gov/mesh/vocab#broaderConcept
    |http://id.nlm.nih.gov/mesh/vocab#broaderDescriptor
    |http://id.nlm.nih.gov/mesh/vocab#broaderQualifier
    |http://id.nlm.nih.gov/mesh/vocab#casn1_label
    |http://id.nlm.nih.gov/mesh/vocab#concept
    |http://id.nlm.nih.gov/mesh/vocab#Concept
    |http://id.nlm.nih.gov/mesh/vocab#considerAlso
    |http://id.nlm.nih.gov/mesh/vocab#dateCreated
    |http://id.nlm.nih.gov/mesh/vocab#dateEstablished
    |http://id.nlm.nih.gov/mesh/vocab#dateRevised
    |http://id.nlm.nih.gov/mesh/vocab#entryVersion
    |http://id.nlm.nih.gov/mesh/vocab#frequency
    |http://id.nlm.nih.gov/mesh/vocab#hasDescriptor
    |http://id.nlm.nih.gov/mesh/vocab#hasQualifier
    |http://id.nlm.nih.gov/mesh/vocab#historyNote
    |http://id.nlm.nih.gov/mesh/vocab#identifier
    |http://id.nlm.nih.gov/mesh/vocab#indexerConsiderAlso
    |http://id.nlm.nih.gov/mesh/vocab#lastActiveYear
    |http://id.nlm.nih.gov/mesh/vocab#lexicalTag
    |http://id.nlm.nih.gov/mesh/vocab#mappedTo
    |http://id.nlm.nih.gov/mesh/vocab#narrowerConcept
    |http://id.nlm.nih.gov/mesh/vocab#nlmClassificationNumber
    |http://id.nlm.nih.gov/mesh/vocab#note
    |http://id.nlm.nih.gov/mesh/vocab#onlineNote
    |http://id.nlm.nih.gov/mesh/vocab#parentTreeNumber
    |http://id.nlm.nih.gov/mesh/vocab#pharmacologicalAction
    |http://id.nlm.nih.gov/mesh/vocab#preferredConcept
    |http://id.nlm.nih.gov/mesh/vocab#preferredMappedTo
    |http://id.nlm.nih.gov/mesh/vocab#preferredTerm
    |http://id.nlm.nih.gov/mesh/vocab#prefLabel
    |http://id.nlm.nih.gov/mesh/vocab#previousIndexing
    |http://id.nlm.nih.gov/mesh/vocab#publicMeSHNote
    |http://id.nlm.nih.gov/mesh/vocab#registryNumber
    |http://id.nlm.nih.gov/mesh/vocab#relatedConcept
    |http://id.nlm.nih.gov/mesh/vocab#relatedRegistryNumber
    |http://id.nlm.nih.gov/mesh/vocab#scopeNote
    |http://id.nlm.nih.gov/mesh/vocab#seeAlso
    |http://id.nlm.nih.gov/mesh/vocab#sortVersion
    |http://id.nlm.nih.gov/mesh/vocab#source
    |http://id.nlm.nih.gov/mesh/vocab#term
    |http://id.nlm.nih.gov/mesh/vocab#thesaurusID
    |http://id.nlm.nih.gov/mesh/vocab#treeNumber
    |http://id.nlm.nih.gov/mesh/vocab#useInstead
    |http://prismstandard.org/namespaces/basic/3.0/contentType
    |http://prismstandard.org/namespaces/basic/3.0/endingPage
    |http://prismstandard.org/namespaces/basic/3.0/issn
    |http://prismstandard.org/namespaces/basic/3.0/issueIdentifier
    |http://prismstandard.org/namespaces/basic/3.0/pageRange
    |http://prismstandard.org/namespaces/basic/3.0/publicationName
    |http://prismstandard.org/namespaces/basic/3.0/startingPage
    |http://purl.obolibrary.org/obo/chebi/charge
    |http://purl.obolibrary.org/obo/chebi/formula
    |http://purl.obolibrary.org/obo/chebi/inchi
    |http://purl.obolibrary.org/obo/chebi/inchikey
    |http://purl.obolibrary.org/obo/chebi/mass
    |http://purl.obolibrary.org/obo/chebi/monoisotopicmass
    |http://purl.obolibrary.org/obo/chebi/smiles
    |http://purl.obolibrary.org/obo/chebi#formula
    |http://purl.obolibrary.org/obo/chebi#mass
    |http://purl.obolibrary.org/obo/IAO_0000114
    |http://purl.obolibrary.org/obo/IAO_0000115
    |http://purl.obolibrary.org/obo/IAO_0000117
    |http://purl.obolibrary.org/obo/IAO_0000118
    |http://purl.obolibrary.org/obo/IAO_0000231
    |http://purl.obolibrary.org/obo/IAO_0000412
    |http://purl.obolibrary.org/obo/IAO_0100001
    |http://purl.obolibrary.org/obo/ncbitaxon#has_rank
    |http://purl.obolibrary.org/obo/RO_0000056
    |http://purl.obolibrary.org/obo/RO_0000087
    |http://purl.org/cerif/frapo/hasFundingAgency
    |http://purl.org/cerif/frapo/isSupportedBy
    |http://purl.org/dc/dcam/domainIncludes
    |http://purl.org/dc/dcam/rangeIncludes
    |http://purl.org/dc/elements/1.1/contributor
    |http://purl.org/dc/elements/1.1/creator
    |http://purl.org/dc/elements/1.1/date
    |http://purl.org/dc/elements/1.1/description
    |http://purl.org/dc/elements/1.1/format
    |http://purl.org/dc/elements/1.1/language
    |http://purl.org/dc/elements/1.1/rights
    |http://purl.org/dc/elements/1.1/title
    |http://purl.org/dc/terms/available
    |http://purl.org/dc/terms/bibliographicCitation
    |http://purl.org/dc/terms/contributor
    |http://purl.org/dc/terms/created
    |http://purl.org/dc/terms/creator
    |http://purl.org/dc/terms/date
    |http://purl.org/dc/terms/description
    |http://purl.org/dc/terms/identifier
    |http://purl.org/dc/terms/isPartOf
    |http://purl.org/dc/terms/issued
    |http://purl.org/dc/terms/language
    |http://purl.org/dc/terms/license
    |http://purl.org/dc/terms/modified
    |http://purl.org/dc/terms/publisher
    |http://purl.org/dc/terms/rights
    |http://purl.org/dc/terms/source
    |http://purl.org/dc/terms/subject
    |http://purl.org/dc/terms/title
    |http://purl.org/ontology/bibo/doi
    |http://purl.org/ontology/bibo/issue
    |http://purl.org/ontology/bibo/pageEnd
    |http://purl.org/ontology/bibo/pageStart
    |http://purl.org/ontology/bibo/pmid
    |http://purl.org/ontology/bibo/volume
    |http://purl.org/pav/2.0/createdBy
    |http://purl.org/pav/2.0/createdOn
    |http://purl.org/spar/cito/discusses
    |http://purl.org/spar/cito/isDiscussedBy
    |http://purl.org/spar/fabio/hasPrimarySubjectTerm
    |http://purl.org/spar/fabio/hasSubjectTerm
    |http://purl.org/vocab/vann/preferredNamespacePrefix
    |http://purl.org/vocab/vann/preferredNamespaceUri
    |http://rdf.ebi.ac.uk/terms/chembl#activityComment
    |http://rdf.ebi.ac.uk/terms/chembl#assayCategory
    |http://rdf.ebi.ac.uk/terms/chembl#assayCellType
    |http://rdf.ebi.ac.uk/terms/chembl#assayStrain
    |http://rdf.ebi.ac.uk/terms/chembl#assaySubCellFrac
    |http://rdf.ebi.ac.uk/terms/chembl#assayTestType
    |http://rdf.ebi.ac.uk/terms/chembl#assayTissue
    |http://rdf.ebi.ac.uk/terms/chembl#assayType
    |http://rdf.ebi.ac.uk/terms/chembl#assayXref
    |http://rdf.ebi.ac.uk/terms/chembl#atcClassification
    |http://rdf.ebi.ac.uk/terms/chembl#bindingSiteName
    |http://rdf.ebi.ac.uk/terms/chembl#cellosaurusId
    |http://rdf.ebi.ac.uk/terms/chembl#chemblId
    |http://rdf.ebi.ac.uk/terms/chembl#classLevel
    |http://rdf.ebi.ac.uk/terms/chembl#classPath
    |http://rdf.ebi.ac.uk/terms/chembl#componentType
    |http://rdf.ebi.ac.uk/terms/chembl#dataValidityComment
    |http://rdf.ebi.ac.uk/terms/chembl#dataValidityIssue
    |http://rdf.ebi.ac.uk/terms/chembl#documentType
    |http://rdf.ebi.ac.uk/terms/chembl#hasActivity
    |http://rdf.ebi.ac.uk/terms/chembl#hasAssay
    |http://rdf.ebi.ac.uk/terms/chembl#hasBindingSite
    |http://rdf.ebi.ac.uk/terms/chembl#hasBioComponent
    |http://rdf.ebi.ac.uk/terms/chembl#hasCellLine
    |http://rdf.ebi.ac.uk/terms/chembl#hasChildMolecule
    |http://rdf.ebi.ac.uk/terms/chembl#hasCLO
    |http://rdf.ebi.ac.uk/terms/chembl#hasDocument
    |http://rdf.ebi.ac.uk/terms/chembl#hasDrugIndication
    |http://rdf.ebi.ac.uk/terms/chembl#hasEFO
    |http://rdf.ebi.ac.uk/terms/chembl#hasEFOName
    |http://rdf.ebi.ac.uk/terms/chembl#hasJournal
    |http://rdf.ebi.ac.uk/terms/chembl#hasMechanism
    |http://rdf.ebi.ac.uk/terms/chembl#hasMesh
    |http://rdf.ebi.ac.uk/terms/chembl#hasMeshHeading
    |http://rdf.ebi.ac.uk/terms/chembl#hasMolecule
    |http://rdf.ebi.ac.uk/terms/chembl#hasParentMolecule
    |http://rdf.ebi.ac.uk/terms/chembl#hasProteinClassification
    |http://rdf.ebi.ac.uk/terms/chembl#hasQUDT
    |http://rdf.ebi.ac.uk/terms/chembl#hasSource
    |http://rdf.ebi.ac.uk/terms/chembl#hasTarget
    |http://rdf.ebi.ac.uk/terms/chembl#hasTargetComponent
    |http://rdf.ebi.ac.uk/terms/chembl#hasTargetComponentDescendant
    |http://rdf.ebi.ac.uk/terms/chembl#hasTargetDescendant
    |http://rdf.ebi.ac.uk/terms/chembl#hasUnitOnto
    |http://rdf.ebi.ac.uk/terms/chembl#helmNotation
    |http://rdf.ebi.ac.uk/terms/chembl#highestDevelopmentPhase
    |http://rdf.ebi.ac.uk/terms/chembl#isBiotherapeutic
    |http://rdf.ebi.ac.uk/terms/chembl#isCellLineForAssay
    |http://rdf.ebi.ac.uk/terms/chembl#isCellLineForTarget
    |http://rdf.ebi.ac.uk/terms/chembl#isTargetForCellLine
    |http://rdf.ebi.ac.uk/terms/chembl#isTargetForMechanism
    |http://rdf.ebi.ac.uk/terms/chembl#mechanismActionType
    |http://rdf.ebi.ac.uk/terms/chembl#mechanismDescription
    |http://rdf.ebi.ac.uk/terms/chembl#moleculeXref
    |http://rdf.ebi.ac.uk/terms/chembl#organismName
    |http://rdf.ebi.ac.uk/terms/chembl#pChembl
    |http://rdf.ebi.ac.uk/terms/chembl#potentialDuplicate
    |http://rdf.ebi.ac.uk/terms/chembl#proteinSequence
    |http://rdf.ebi.ac.uk/terms/chembl#relation
    |http://rdf.ebi.ac.uk/terms/chembl#relHasSubset
    |http://rdf.ebi.ac.uk/terms/chembl#relOverlapsWith
    |http://rdf.ebi.ac.uk/terms/chembl#relSubsetOf
    |http://rdf.ebi.ac.uk/terms/chembl#standardRelation
    |http://rdf.ebi.ac.uk/terms/chembl#standardType
    |http://rdf.ebi.ac.uk/terms/chembl#standardUnits
    |http://rdf.ebi.ac.uk/terms/chembl#standardValue
    |http://rdf.ebi.ac.uk/terms/chembl#substanceType
    |http://rdf.ebi.ac.uk/terms/chembl#targetCmptXref
    |http://rdf.ebi.ac.uk/terms/chembl#targetConfDesc
    |http://rdf.ebi.ac.uk/terms/chembl#targetConfScore
    |http://rdf.ebi.ac.uk/terms/chembl#targetRelDesc
    |http://rdf.ebi.ac.uk/terms/chembl#targetRelType
    |http://rdf.ebi.ac.uk/terms/chembl#targetType
    |http://rdf.ebi.ac.uk/terms/chembl#taxonomy
    |http://rdf.ebi.ac.uk/terms/chembl#type
    |http://rdf.ebi.ac.uk/terms/chembl#units
    |http://rdf.ebi.ac.uk/terms/chembl#value
    |http://rdf.ncbi.nlm.nih.gov/pubchem/vocabulary#discussesAsDerivedByTextMining
    |http://rdf.ncbi.nlm.nih.gov/pubchem/vocabulary#has_parent
    |http://rdf.ncbi.nlm.nih.gov/pubchem/vocabulary#is_active_ingredient_of
    |http://rdf.wwpdb.org/schema/pdbx-v40.owl#link_to_pdb
    |http://rdfs.org/ns/void#dataDump
    |http://rdfs.org/ns/void#distinctSubjects
    |http://rdfs.org/ns/void#entities
    |http://rdfs.org/ns/void#exampleResource
    |http://rdfs.org/ns/void#feature
    |http://rdfs.org/ns/void#subset
    |http://rdfs.org/ns/void#triples
    |http://rdfs.org/ns/void#uriLookupEndpoint
    |http://rdfs.org/ns/void#uriSpace
    |http://rdfs.org/ns/void#vocabulary
    |http://semanticscience.org/resource/CHEMINF_000455
    |http://semanticscience.org/resource/CHEMINF_000461
    |http://semanticscience.org/resource/CHEMINF_000462
    |http://semanticscience.org/resource/CHEMINF_000477
    |http://semanticscience.org/resource/CHEMINF_000480
    |http://semanticscience.org/resource/SIO_000008
    |http://semanticscience.org/resource/SIO_000011
    |http://semanticscience.org/resource/SIO_000221
    |http://semanticscience.org/resource/SIO_000300
    |http://voag.linkedmodel.org/schema/voag#frequencyOfChange
    |http://www.bioassayontology.org/bao#BAO_0000205
    |http://www.bioassayontology.org/bao#BAO_0000208
    |http://www.geneontology.org/formats/oboInOwl#date
    |http://www.geneontology.org/formats/oboInOwl#default-namespace
    |http://www.geneontology.org/formats/oboInOwl#hasAlternativeId
    |http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym
    |http://www.geneontology.org/formats/oboInOwl#hasDbXref
    |http://www.geneontology.org/formats/oboInOwl#hasDefinition
    |http://www.geneontology.org/formats/oboInOwl#hasExactSynonym
    |http://www.geneontology.org/formats/oboInOwl#hasOBOFormatVersion
    |http://www.geneontology.org/formats/oboInOwl#hasOBONamespace
    |http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym
    |http://www.geneontology.org/formats/oboInOwl#hasScope
    |http://www.geneontology.org/formats/oboInOwl#hasSynonym
    |http://www.geneontology.org/formats/oboInOwl#hasSynonymType
    |http://www.geneontology.org/formats/oboInOwl#id
    |http://www.geneontology.org/formats/oboInOwl#inSubset
    |http://www.geneontology.org/formats/oboInOwl#is_cyclic
    |http://www.geneontology.org/formats/oboInOwl#is_transitive
    |http://www.geneontology.org/formats/oboInOwl#saved-by
    |http://www.geneontology.org/formats/oboInOwl#shorthand
    |http://www.geneontology.org/formats/oboInOwl#source
    |http://www.metabohub.org/semantics/resource/2022#taxon
    |http://www.semanticweb.org/ontologies/cheminf.owl#short_name
    |http://www.w3.org/1999/02/22-rdf-syntax-ns#first
    |http://www.w3.org/1999/02/22-rdf-syntax-ns#rest
    |http://www.w3.org/1999/02/22-rdf-syntax-ns#type
    |http://www.w3.org/2000/01/rdf-schema#comment
    |http://www.w3.org/2000/01/rdf-schema#domain
    |http://www.w3.org/2000/01/rdf-schema#isDefinedBy
    |http://www.w3.org/2000/01/rdf-schema#label
    |http://www.w3.org/2000/01/rdf-schema#range
    |http://www.w3.org/2000/01/rdf-schema#seeAlso
    |http://www.w3.org/2000/01/rdf-schema#subClassOf
    |http://www.w3.org/2000/01/rdf-schema#subPropertyOf
    |http://www.w3.org/2001/XMLSchema#pattern
    |http://www.w3.org/2002/07/owl#allValuesFrom
    |http://www.w3.org/2002/07/owl#annotatedProperty
    |http://www.w3.org/2002/07/owl#annotatedSource
    |http://www.w3.org/2002/07/owl#annotatedTarget
    |http://www.w3.org/2002/07/owl#cardinality
    |http://www.w3.org/2002/07/owl#deprecated
    |http://www.w3.org/2002/07/owl#disjointWith
    |http://www.w3.org/2002/07/owl#equivalentClass
    |http://www.w3.org/2002/07/owl#equivalentProperty
    |http://www.w3.org/2002/07/owl#imports
    |http://www.w3.org/2002/07/owl#intersectionOf
    |http://www.w3.org/2002/07/owl#inverseOf
    |http://www.w3.org/2002/07/owl#members
    |http://www.w3.org/2002/07/owl#onDataRange
    |http://www.w3.org/2002/07/owl#onDatatype
    |http://www.w3.org/2002/07/owl#onProperty
    |http://www.w3.org/2002/07/owl#priorVersion
    |http://www.w3.org/2002/07/owl#propertyChainAxiom
    |http://www.w3.org/2002/07/owl#propertyDisjointWith
    |http://www.w3.org/2002/07/owl#qualifiedCardinality
    |http://www.w3.org/2002/07/owl#sameAs
    |http://www.w3.org/2002/07/owl#seeAlso
    |http://www.w3.org/2002/07/owl#someValuesFrom
    |http://www.w3.org/2002/07/owl#unionOf
    |http://www.w3.org/2002/07/owl#versionInfo
    |http://www.w3.org/2002/07/owl#versionIRI
    |http://www.w3.org/2002/07/owl#withRestrictions
    |http://www.w3.org/2004/02/skos/core#altLabel
    |http://www.w3.org/2004/02/skos/core#broader
    |http://www.w3.org/2004/02/skos/core#closeMatch
    |http://www.w3.org/2004/02/skos/core#definition
    |http://www.w3.org/2004/02/skos/core#exactMatch
    |http://www.w3.org/2004/02/skos/core#example
    |http://www.w3.org/2004/02/skos/core#narrower
    |http://www.w3.org/2004/02/skos/core#prefLabel
    |http://www.w3.org/2004/02/skos/core#relatedMatch
    |http://www.w3.org/2004/02/skos/core#scopeNote
    |http://xmlns.com/foaf/0.1/depiction
    |http://xmlns.com/foaf/0.1/homepage
    |http://xmlns.com/foaf/0.1/primaryTopic
    |https://rdf.metanetx.org/schema/canGrow
    |https://rdf.metanetx.org/schema/cata
    |https://rdf.metanetx.org/schema/charge
    |https://rdf.metanetx.org/schema/chem
    |https://rdf.metanetx.org/schema/chemCount
    |https://rdf.metanetx.org/schema/chemInMNXref
    |https://rdf.metanetx.org/schema/chemIsom
    |https://rdf.metanetx.org/schema/chemRefer
    |https://rdf.metanetx.org/schema/chemReplacedBy
    |https://rdf.metanetx.org/schema/chemSource
    |https://rdf.metanetx.org/schema/chemXref
    |https://rdf.metanetx.org/schema/classification
    |https://rdf.metanetx.org/schema/coef
    |https://rdf.metanetx.org/schema/comp
    |https://rdf.metanetx.org/schema/compCount
    |https://rdf.metanetx.org/schema/compInMNXref
    |https://rdf.metanetx.org/schema/compRefer
    |https://rdf.metanetx.org/schema/compSource
    |https://rdf.metanetx.org/schema/compXref
    |https://rdf.metanetx.org/schema/cplx
    |https://rdf.metanetx.org/schema/default_LB
    |https://rdf.metanetx.org/schema/default_UB
    |https://rdf.metanetx.org/schema/dir
    |https://rdf.metanetx.org/schema/formula
    |https://rdf.metanetx.org/schema/geneName
    |https://rdf.metanetx.org/schema/gpr
    |https://rdf.metanetx.org/schema/hasIsomericChild
    |https://rdf.metanetx.org/schema/inchi
    |https://rdf.metanetx.org/schema/inchikey
    |https://rdf.metanetx.org/schema/isBalanced
    |https://rdf.metanetx.org/schema/isTransport
    |https://rdf.metanetx.org/schema/lb
    |https://rdf.metanetx.org/schema/left
    |https://rdf.metanetx.org/schema/lineage
    |https://rdf.metanetx.org/schema/mass
    |https://rdf.metanetx.org/schema/mnxr
    |https://rdf.metanetx.org/schema/organism
    |https://rdf.metanetx.org/schema/pept
    |https://rdf.metanetx.org/schema/peptCount
    |https://rdf.metanetx.org/schema/peptRefer
    |https://rdf.metanetx.org/schema/peptSource
    |https://rdf.metanetx.org/schema/peptXref
    |https://rdf.metanetx.org/schema/reac
    |https://rdf.metanetx.org/schema/reacCount
    |https://rdf.metanetx.org/schema/reacInMNXref
    |https://rdf.metanetx.org/schema/reacRefer
    |https://rdf.metanetx.org/schema/reacReplacedBy
    |https://rdf.metanetx.org/schema/reacSource
    |https://rdf.metanetx.org/schema/reacXref
    |https://rdf.metanetx.org/schema/right
    |https://rdf.metanetx.org/schema/smiles
    |https://rdf.metanetx.org/schema/specCount
    |https://rdf.metanetx.org/schema/subu
    |https://rdf.metanetx.org/schema/taxid
    |https://rdf.metanetx.org/schema/ub
    |
    |""".stripMargin.toList


 */
