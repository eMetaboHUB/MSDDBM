package fr.inrae.msd

import net.sansa_stack.rdf.spark.model.GraphLoader
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.Property
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.util.{Failure, Success, Try}

object GraphXTools {


  /** Searches recursively a property (mimics SPARQL property paths).
   * @param graph input RDF graph
   * @param property property to search recursively
   * @param fromSubject True if the search goes from subjects to objects recursively, false otherwise
   * @param startNodeUri URI of the initial node to search from
   * @param maxIterations max depth of the search
   * @return a new graph holding every visited node, in the form
   *         nodeURI http://inrae.fr/propertySubGraphElementOf startNodeUri
   */
  def searchPropertySubGraphAsGraph(graph : RDD[Triple], property : String, fromSubject : Boolean, startNodeUri : String, maxIterations : Int = 10): RDD[Triple] = {
    def nodeToTriple(node: Node): Triple = {
      val pred = NodeFactory.createURI("http://inrae.fr/propertySubGraphElementOf")
      val obj = NodeFactory.createURI(startNodeUri)
      new Triple(node, pred, obj)
    }
    searchPropertySubGraph(graph, property, fromSubject, startNodeUri, maxIterations).map(nodeToTriple)
  }

  /** Searches recursively a property (mimics SPARQL property paths).
   * @param graph input RDF graph
   * @param property property to search recursively
   * @param fromSubject True if the search goes from subjects to objects recursively, false otherwise
   * @param startNodeUri URI of the initial node to search from
   * @param maxIterations max depth of the search
   * @return an RDD holding every visited node
   *
   */

  def searchPropertySubGraph(graph : RDD[Triple], property : String, fromSubject : Boolean, startNodeUri : String, maxIterations : Int = 10): RDD[Node] = {
    val propertyAsNode = NodeFactory.createURI(property)
    val propertyGraph = graph
      .filter(triple => triple.predicateMatches(propertyAsNode))
      .asHashedGraph()

    val startVertexId = findNodeId(propertyGraph,startNodeUri).getOrElse(throw new NoSuchElementException(s"vertex ID not found in filtered graph with only triples with predicate $property  remaining"))

    if (fromSubject) {
      searchFromSubject(propertyGraph, startVertexId, maxIterations)
    } else {
      searchFromObject(propertyGraph, startVertexId, maxIterations)
    }
  }


  /** Searches recursively a property from right(objects) to left(subjects) (mimics SPARQL property paths).
   * @param graph input RDF graph in GraphX format
   * @param startVertexId ID of the initial node to search from
   * @param maxIterations max depth of the search
   * @return an RDD holding every visited node
   *
   */
  def searchFromObject(graph: Graph[Node, Node], startVertexId: VertexId, maxIterations : Int = 10 ): RDD[Node] = { //descendants with subClassOf
    searchGraph(graph,startVertexId,EdgeDirection.In,maxIterations)
  }

  /** Searches recursively a property from left(subjects) to right(objects) (mimics SPARQL property paths).
   * @param graph input RDF graph in GraphX format
   * @param startVertexId ID of the initial node to search from
   * @param maxIterations max depth of the search
   * @return an RDD holding every visited node
   *
   */
  def searchFromSubject(graph: Graph[Node, Node], startVertexId: VertexId, maxIterations: Int = 10): RDD[Node] = { //ancestors with subClassOf
    searchGraph(graph,startVertexId,EdgeDirection.Out,maxIterations)
  }

  // Define a case class to hold the vertex properties

  /** Searches recursively a property with Pregel (mimics SPARQL property paths).
   * @param graph input RDF graph in GraphX format
   * @param startVertexId ID of the initial node to search from
   * @param searchDirection Either EdgeDirection.Out or EdgeDirection.In, resp. subject to object or object to subject in an RDF context
   * @param maxIterations max depth of the search
   * @return an RDD holding every visited node
   *
   */


  private def searchGraph(graph: Graph[Node, Node], startVertexId: VertexId, searchDirection: EdgeDirection, maxIterations: Int = 10): RDD[Node] = {


    // Initialize the graph, marking the start vertex as visited
    val initialGraph = graph.mapVertices { case (id, node) =>
      VertexAttribute(node, id == startVertexId)
    }

    val initialMessage = false // At first, all are marked as "not visited"

    // Vertex program to update the vertex attribute (visited) based on the incoming message
    def vertexProgram(id: VertexId, attr: VertexAttribute, msg: Boolean): VertexAttribute = {
      VertexAttribute(attr.node, attr.visited || msg)
    }

    // Send message function to propagate the visit information based on the search direction
    def sendMessage(edge: EdgeTriplet[VertexAttribute, _]): Iterator[(VertexId, Boolean)] = {
      searchDirection match {
        case EdgeDirection.Out => // Searching for ancestors
          if (edge.srcAttr.visited) Iterator((edge.dstId, true)) else Iterator.empty
        case EdgeDirection.In => // Searching for descendants
          if (edge.dstAttr.visited) Iterator((edge.srcId, true)) else Iterator.empty
        case _ => // If needed, handle other directions (both)
          Iterator.empty
      }
    }

    // Merge message function to combine messages received by a vertex
    def mergeMessage(msg1: Boolean, msg2: Boolean): Boolean = msg1 || msg2 // We only care about activation messages

    // Execute Pregel to mark all relevant vertices (ancestors or descendants)
    val result = initialGraph.pregel(initialMessage, maxIterations, searchDirection)(
      vertexProgram,
      sendMessage,
      mergeMessage
    )

    // Filter the vertices to get those that have been visited (excluding the start vertex)
    val visitedVertices = result.vertices.filter { case (id, attr) => attr.visited && id != startVertexId }

    // Map the visited vertices to get the original node properties
    visitedVertices.map { case (_, attr) => attr.node }
  }

  case class VertexAttribute(node: Node, visited: Boolean)

  /**  Finds the vertex ID associated with a specific node URI
   * @param graph input graph
   * @param nodeIri IRI we want to find
   * @return the relevant vertex ID or None
   */
  def findNodeId(graph: Graph[Node, Node], nodeIri: String): Option[VertexId] = {

     val id= Try(graph.vertices
        .filter { case (_, node) => node.hasURI(nodeIri) }
        .map(_._1)
        .first())

    id match {
      case Success(value) => Some(value)
      case Failure(exception) => print(s"Could not find vertex ID associated with $nodeIri: ${exception.getMessage}")
        None
    }
  }
}
