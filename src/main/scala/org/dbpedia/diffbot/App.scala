package org.dbpedia.diffbot


import java.io.ByteArrayOutputStream
import org.apache.jena.query.{Query, ResultSetFormatter}
import _root_.org.apache.jena.rdf.model.ModelFactory
import com.typesafe.config.ConfigFactory

import sys.process._
/**
 * @author ${user.name}
 */
object App {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)


  def main(args : Array[String]): Unit = {
    val datasets = DiffUtils.generateConfigDatasets()

    //val sparqlResult = DiffUtils.getDownloadURLS("https://downloads.dbpedia.org/tmpdev/dbpedia-synth/mappings-synth/specific-mappingbased-properties/2019.04.07/dataid.ttl")
    val versionMap = DiffUtils.getAllArtifactVersions("specific-mappingbased-properties", "mappings-synth")
    for (version <- versionMap.keys.toList.sortBy(keys => keys)) {println("Version: "+version+" und Graph: "+versionMap(version))}
    DiffUtils.downloadFiles(DiffUtils.getDownloadURLS(versionMap("2019.04.13")), DiffUtils.readStringFromConfig("cnfg.localDir"))


  }

}
