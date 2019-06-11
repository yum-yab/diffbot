package org.dbpedia.diffbot
import com.typesafe.config.ConfigFactory
import org.apache.jena.query.{Query, QueryExecution, QueryExecutionFactory, ResultSet}
import org.slf4j.LoggerFactory

object SparqlClient {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def sendQuery (query: Query, endpoint : String): ResultSet = {
    try {

      val execution = QueryExecutionFactory.sparqlService(endpoint, query)
      val resultSet = execution.execSelect()
      resultSet
    } catch {
      case e: Exception => {
        e.printStackTrace()
        logger.error("Couldnt reach sparql endpoint")
        null
      }
    }
  }
}
