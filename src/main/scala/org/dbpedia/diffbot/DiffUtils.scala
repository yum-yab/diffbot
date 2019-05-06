package org.dbpedia.diffbot

import java.io.ByteArrayOutputStream

import com.typesafe.config.ConfigFactory
import org.apache.jena.query.{ParameterizedSparqlString, QueryExecution, QueryExecutionFactory, ResultSet, ResultSetFormatter}
import org.slf4j.{Logger, LoggerFactory}
import sys.process._
import java.net.URL
import java.io.File
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON

object DiffUtils {

  final val config = ConfigFactory.load("diffbot.conf")
  final val logger = LoggerFactory.getLogger(this.getClass)


  def readStringFromConfig(id : String): String = {
    config.getString(id)
  }

  //TODO: replace with better config maybe
  /**
    * Used for reading the config
    * @return
    */

  def generateConfigDatasets(): List[Dataset] = {
    var datasets = new ListBuffer[Dataset]()
    for (dataset <- config.getStringList("datasets.supervised").asScala) {
      datasets += new Dataset(dataset, config.getStringList("datasets."+dataset+".artifacts").asScala.toList, config.getStringList("datasets."+dataset+".content-variants").asScala.toList)
    }
    datasets.toList
  }

  /**
    * Connects to the SPARQL Endpint to get all the versions of an Artifact and the related Graph (databus-dataid)
    * @param artifact
    * @param datasetname
    * @return
    */


  def getAllArtifactVersions(artifact : String, datasetname : String): Map[String,String] = {

    val user = config.getString("cnfg.releaser")
    val endpoint = config.getString("cnfg.endpoint")

    val query = new ParameterizedSparqlString(
      "PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
        "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
        "PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>\n\n" +
        "SELECT DISTINCT ?version ?g WHERE {\n  " +
        "{GRAPH ?g  { " +
        "<https://databus.dbpedia.org/" + user + "/" + datasetname + "/" + artifact + "> rdf:type dataid:Artifact .\n" +
        "?version rdf:type dataid:Version . \n}" +
        "}\n" +
        "} ").asQuery()
    val resultSet = SparqlClient.sendQuery(query)
    var resultMap = Map[String, String]()
    while (resultSet.hasNext) {
      val solution = resultSet.next()
      resultMap += (getVersionFromUri(solution.getResource("?version").getURI) -> solution.getResource("?g").getURI)
    }
    if (resultMap.isEmpty) {logger.warn(printf("No Versions found for %a in %d.", artifact, datasetname).toString)}
    resultMap
  }

  /**
    * Returns the dowload URLS for this specific graph (dataid of dbpedia-databus)
    * @param graph
    * @return
    */
  def getDownloadURLS (graph : String): List[String] = {
    val query = new ParameterizedSparqlString("PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
      "PREFIX dct:    <http://purl.org/dc/terms/>\n" +
      "PREFIX dcat:   <http://www.w3.org/ns/dcat#>\n" +
      "PREFIX db:     <https://databus.dbpedia.org/>\n" +
      "PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
      "PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>\n\n" +
      "SELECT DISTINCT ?URL " +
      "WHERE {\n" +
      "  GRAPH <"+graph+"> " +
      "{?file dcat:downloadURL ?URL}" +
      "\n} ").asQuery()

    val resultSet = SparqlClient.sendQuery(query)
    var resultList = List[String]()

    while (resultSet.hasNext) {
      resultList = (resultSet.next().getResource("?URL").getURI) :: resultList
    }
    if (resultList.isEmpty) {logger.warn("No Download URLS in this Graph")}
    resultList
  }

  def downloadFiles (urls : List[String], targetpath : String): Unit = {
    for (url <- urls) {
      val filename = url.split("/")(url.split("/").length-1)
      downloadFile(url, targetpath+filename)
    }

  }

  private def getVersionFromUri (uri : String) : String ={
    val urisplit = uri.split("/")
    urisplit(urisplit.length-1)
  }

  /**
    * Downloads a File to the given Path
    * @param url File-URL
    * @param path Path to save to
    * @return False if an Error ocurred, else true
    */

  private def downloadFile (url : String, path : String) : Boolean = {
    try {
      new URL(url) #> new File(path) !!;
      logger.info("Successfully downloaded to "+path)
      true
    } catch {
      case e : Exception => {
        logger.error(printf("There was a problem Downloading and saving the File from %u to %p.", url, path).toString)
        false
      }
    }

  }

}
