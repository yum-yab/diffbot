package org.dbpedia.diffbot

import java.io.ByteArrayOutputStream

import com.typesafe.config.ConfigFactory
import org.apache.jena.query.{ParameterizedSparqlString, QueryExecution, QueryExecutionFactory, QuerySolution, ResultSet, ResultSetFormatter}
import org.slf4j.{Logger, LoggerFactory}

import sys.process._
import java.net.URL
import java.io.File
import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object DiffUtils {

  final val config = ConfigFactory.load("diffbot.conf")
  final val logger = LoggerFactory.getLogger(this.getClass)
  final val dateFormat = new SimpleDateFormat("yyyy.MM.dd")

  def readStringFromConfig(id : String): String = {
    config.getString(id)
  }

  //TODO: replace with better config maybe
  /**
    * Used for reading the config
    * @return
    */

  def generateConfigDatasets(): List[Dataset] = {
    val datasetNames : List[String] = config.getStringList("datasets.supervised").asScala.toList
    for (datasetName <- datasetNames) yield {
      new Dataset(datasetName, getResources(datasetName))
    }
  }



  /**
    * Sends SPARQL-Query to the dbpedia endpoint to check for all the versions of it
    * @param datasetName
    * @return a list with the tuple (artifact, version, downloadURL) later then lastVersion, sorted by the version
    */
  def getResources (datasetName : String): List[(String, String, String)] = {

    logger.info("Getting the resources for "+datasetName)
    val user = config.getString("datasets.releaser")
    val endpoint = config.getString("datasets.endpoint")
    val diffReleases = getDiffReleases()


    val query = new ParameterizedSparqlString("" +
      "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
      "PREFIX dct:    <http://purl.org/dc/terms/>\n" +
      "PREFIX dcat:   <http://www.w3.org/ns/dcat#>\n" +
      "PREFIX db:     <https://databus.dbpedia.org/>\n" +
      "PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
      "PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>\n\n" +
      "SELECT DISTINCT ?artifact ?version ?URL WHERE {\n" +
      s"  GRAPH ?g {\n    <https://databus.dbpedia.org/$user/$datasetName> rdf:type dataid:Group .\n" +
      "    ?artifact rdf:type dataid:Artifact .\n" +
      "    ?version rdf:type dataid:Version .\n" +
      "    ?c dcat:downloadURL ?URL\n" +
      "  }\n" +
      "} \n" +
      "ORDER BY ?artifact").asQuery()
    val resultList = ResultSetFormatter.toList(SparqlClient.sendQuery(query, endpoint)).asScala.sortBy(solution => getIdentifier(solution.getResource("?version").getURI))

    val lastVersion = diffReleases.reverse.head match {
      case null => getIdentifier(resultList.head.getResource("?version").getURI)
      case _ => diffReleases.reverse.head
    }



    val results = for (solution <- resultList if (getIdentifier(solution.getResource("?version").getURI).compareTo(lastVersion) >= 0)) yield {
      val artifact = DiffUtils.getIdentifier(solution.getResource("?artifact").getURI)
      val version = DiffUtils.getIdentifier(solution.getResource("?version").getURI)
      val URL = solution.getResource("?URL").getURI
      (artifact, version, URL)
    }

    if (resultList.isEmpty) {logger.warn(s"There are no released versions for $datasetName from $user.")}
    else {logger.info(s"Found ${results.size} files for dataset $datasetName.")}
    results.toList
  }


  /**
    * Downloads a File to the given Path (copied from StackOverflow)
    * @param url File-URL
    * @param path Path to save to
    * @return False if an Error ocurred, else true
    */

  def downloadFile (url : String, path : String) : Boolean = {
    try {
      new URL(url) #> new File(path) !!;
      logger.info(s"Successfully downloaded to $path")
      true
    } catch {
      case e : Exception => {
        logger.error(s"There was a problem Downloading and saving the File from $url to $path.")
        false
      }
    }

  }

  def getDiffReleases (): List[String] = {

    val user = config.getString("diff.releaser")
    val diffId = config.getString("diff.diffId")
    val endpoint = config.getString("diff.endpoint")

    val query = new ParameterizedSparqlString("" +
      "PREFIX  dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
      "PREFIX  dct:  <http://purl.org/dc/terms/>\n" +
      "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
      "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
      "PREFIX  dcat: <http://www.w3.org/ns/dcat#>\n" +
      "PREFIX  db:   <https://databus.dbpedia.org/>\n" +
      "PREFIX  prov: <http://www.w3.org/ns/prov#>\n\n" +
      "SELECT DISTINCT ?version\n" +
      "WHERE\n" +
      "  { GRAPH ?g\n" +
      "      { " +
      "         ?s prov:wasDerivedFrom ?version." +
      s"         <https://databus.dbpedia.org/$user/$diffId> rdf:type dataid:Group ." +
      "      }\n" +
      "  }").asQuery()

    val resultList = ResultSetFormatter.toList(SparqlClient.sendQuery(query, endpoint)).asScala

    (for (solution <- resultList) yield {getIdentifier(solution.getResource("?version").getURI)}).sortBy(version => version).toList
  }

  def getIdentifier (url : String): String = {
    url.split("/").reverse.head
  }

}
