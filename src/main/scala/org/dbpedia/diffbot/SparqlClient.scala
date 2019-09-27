package org.dbpedia.diffbot
import org.apache.jena.query.{Query, QueryExecution, QueryExecutionFactory, QuerySolution, ResultSet, ResultSetFormatter}
import org.apache.jena.sparql.engine.http.QueryEngineHTTP
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._


object SparqlClient {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def sendQuery (query: Query, endpoint : String): List[QuerySolution] = {
    try {
      val execution = QueryExecutionFactory.sparqlService(endpoint, query)
      val exec = new QueryEngineHTTP(endpoint, query)
      val results = ResultSetFormatter.toList(exec.execSelect()).asScala
      exec.close()
      results.toList
    } catch {
      case e: Exception => {
        e.printStackTrace()
        logger.error("Couldn't reach sparql endpoint")
        null
      }
    }
  }
}
