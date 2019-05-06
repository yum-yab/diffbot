package org.dbpedia.diffbot
import com.typesafe.config.ConfigFactory
import org.apache.jena.query.{Query, QueryExecution, QueryExecutionFactory, ResultSet}
import org.slf4j.LoggerFactory

object SparqlClient {

  final val logger = LoggerFactory.getLogger(this.getClass)

  final val endpoint = ConfigFactory.load("diffbot.conf").getString("cnfg.endpoint")

  def sendQuery (query: Query): ResultSet = {
    try {

      val execution = QueryExecutionFactory.sparqlService(endpoint, query)
      val resultSet = execution.execSelect()
      return resultSet
    } catch {
      case e: Exception => {
        e.printStackTrace()
        logger.error("Couldnt reach sparql endpoint")
        return null
      }
    }
  }
}
