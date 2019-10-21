package org.dbpedia.diffbot

import org.apache.jena.query.{ParameterizedSparqlString, Query, QuerySolution}
import org.dbpedia.diffbot.DiffUtils.getIdentifier
import org.slf4j.LoggerFactory

class DatasetGenerator (config : Config) {

  final val logger = LoggerFactory.getLogger(this.getClass)

  //TODO: replace with better config maybe
  /**
   * Used for reading the config
   * @return
   */

  def generateConfigDatasets(): List[Dataset] = {
    for (datasetName <- config.dataset.datasetNames) yield {
      logger.info(s"Looking for the new diff versions...")
      val diffVersions = getDiffVersions(datasetName)
      val oldVersion = diffVersions._1
      val newVersion = diffVersions._2
      logger.info(s"New diff-versions: ${oldVersion} and ${newVersion}")

      logger.info(s"Getting the old ressources from $datasetName, version $oldVersion")
      val oldResources = getResources(datasetName, oldVersion)
      logger.info(s"Getting the new ressources from $datasetName, version $newVersion")
      val newResources = getResources(datasetName, newVersion)

      val artifacts = (for (result <- oldResources) yield result.getResource("?artifact").getURI).distinct

      val artifactList = for (artifact <- artifacts) yield {
        val oldDbpediaFiles = for (solution <- oldResources if solution.getResource("?artifact").getURI == artifact)
          yield DBpediaFile(
            getFileName(solution.getResource("?file").getURI),
            solution.getResource("?URL").getURI
          )
        val newDbpediaFiles = for (solution <- newResources if solution.getResource("?artifact").getURI == artifact)
          yield DBpediaFile(
            fileName = getFileName(solution.getResource("?file").getURI),
            downloadURL = solution.getResource("?URL").getURI
          )
        DBpediaArtifact(
          name = getIdentifier(artifact),
          url = artifact,
          oldResources = oldDbpediaFiles,
          newResources = newDbpediaFiles
        )
      }
      Dataset(
        name = datasetName,
        resources = artifactList,
        oldVersion = oldVersion,
        newVersion = newVersion
      )
    }
  }

  /**
   * Sends SPARQL-Query to the dbpedia endpoint to check for all the versions of it
   * @param datasetName
   * @return a list with the tuple (artifact, version, downloadURL) later then lastVersion, sorted by the version
   */
  private def getResources (datasetName : String, version : String): List[QuerySolution] = {

    val query : Query = new ParameterizedSparqlString("" +
      "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
      "PREFIX dct:    <http://purl.org/dc/terms/>\n" +
      "PREFIX dcat:   <http://www.w3.org/ns/dcat#>\n" +
      "PREFIX db:     <https://databus.dbpedia.org/>\n" +
      "PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
      "PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>\n\n" +
      "SELECT DISTINCT ?artifact ?file ?URL WHERE {\n" +
      s"  GRAPH ?g {\n    <https://databus.dbpedia.org/${config.dataset.releaser}/$datasetName> rdf:type dataid:Group .\n" +
      "    ?artifact rdf:type dataid:Artifact .\n" +
      s"   ?version rdf:type dataid:Version .\n" +
      s"   ?file rdf:type dataid:SingleFile ." +
      "    ?file dcat:downloadURL ?URL.\n" +
      "   filter contains(str(?version),\""+version+"\") \n" +
      "  }\n" +
      "} \n" +
      "ORDER BY ?artifact").asQuery()

    SparqlClient.sendQuery(query, config.dataset.endpoint)
  }

  private def getDiffVersions (dataset: String): (String, String) = {
    val versionRegex = DiffUtils.versionRegex.unanchored
    val releasedVersions = getLastReleases(dataset)
    val diffedVersions = getDiffReleases(dataset)

    val lastVersion = diffedVersions.head match {
      case null => releasedVersions.head
      case versionRegex(date) => date
    }

    val newVersions = (for (releasedVersion <- releasedVersions; if releasedVersion > lastVersion) yield releasedVersion).distinct.sortBy(version => version)
    if (newVersions.isEmpty) {
      logger.info(s"There are no new Versions to diff.")
      System.exit(0)
      (null,null)
    } else {
      (lastVersion, newVersions(0))
    }
  }

  private def getLastReleases(datasetName :String): List[String] = {

    val query : Query = new ParameterizedSparqlString("" +
      "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
      "PREFIX dct:    <http://purl.org/dc/terms/>\n" +
      "PREFIX dcat:   <http://www.w3.org/ns/dcat#>\n" +
      "PREFIX db:     <https://databus.dbpedia.org/>\n" +
      "PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
      "PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>\n\n" +
      "SELECT DISTINCT ?version WHERE {\n" +
      s"  GRAPH ?g {\n    <https://databus.dbpedia.org/${config.dataset.releaser}/$datasetName> rdf:type dataid:Group .\n" +
      "    ?version rdf:type dataid:Version .\n" +
      "  }\n" +
      "} \n" +
      "ORDER BY DESC(?version)").asQuery()

    val resultList = SparqlClient.sendQuery(query, config.dataset.endpoint)
    (for (result <- resultList) yield {getIdentifier(result.getResource("?version").getURI)}).sortBy( version => version).toList.distinct
  }

  private def getDiffReleases (datasetName : String): List[String] = {

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
      s"         <https://databus.dbpedia.org/${config.diff.releaser}/${config.diff.diffId}> rdf:type dataid:Group ." +
      "      }\n" +
      "  }").asQuery()

    val resultList = SparqlClient.sendQuery(query, config.diff.endpoint)

    (for (solution <- resultList if solution.getResource("?version").getURI.contains(datasetName)) yield {getIdentifier(solution.getResource("?version").getURI)}).sortBy(version => version).reverse.distinct
  }


  private def getFileName (singleFileUri : String): String = {
    val singleFilePattern = """.*#((.*?)(_[\w-=]+)*(\.[\w]+)(\.[\w]+)?)""".r

    singleFileUri match {
      case singleFilePattern(filename, _*) => filename
      case _ => null
    }
  }

}
