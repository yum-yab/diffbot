package org.dbpedia.diffbot

import java.io
import java.io.FileWriter
import java.util.Calendar

import scala.reflect.io.{Directory, File}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import sys.process._
import org.apache.maven.shared.invoker._

import scala.collection.SortedSet

/**
  * Handle one Diff
  * @param datasets - List of datasets
  * @param diffVersion - version on which the diff should be released (default todays date)
  * @param diffId - name of the diff
  */

class DiffHandler(val datasets : List[Dataset] ,val diffVersion : String = DiffUtils.dateFormat.format(Calendar.getInstance().getTime()), val diffId : String = DiffUtils.readStringFromConfig("diff.diffId")) {

  // parent directory of the poms
  private val localDir = DiffUtils.readStringFromConfig("cnfg.localDir")
  // directory with all the scripts
  private val scriptdir = ConfigFactory.load("diffbot.conf").getString("cnfg.scripts")
  final val logger = LoggerFactory.getLogger(this.getClass)


  def handleDatasets(): Unit = {
    //TODO: Write this method
    //handle artifact


    // Downloading the files and generating the directory structure
    for (dataset <- datasets) {
      for (result <- dataset.resources if (result._2 == dataset.oldVersion || result._2 == dataset.newVersion)) {
        val path = localDir + "/" + dataset.name + "/" + DiffUtils.getIdentifier(result._1) + "/" + DiffUtils.getIdentifier(result._2) + "/"
        saveDirCreation(path)
        val filename = DiffUtils.getIdentifier(result._3)
        DiffUtils.downloadFile(result._3, path + filename)
      }
    }

    //run the diff
    saveDirCreation(localDir+"/"+diffId)
    for (dataset <- datasets) {
      logger.info("Started running the diff of "+dataset.name+"...")
      if (!runDiff(localDir + "/" + dataset.name, dataset.oldVersion, dataset.newVersion)) {
        logger.error(printf("There was an error in diffing the Versions %s and %s of %s.", dataset.oldVersion, dataset.newVersion, dataset.name).toString)
      } else {
        // add the lines to the provenance file
        val baseURL = getBaseURL(dataset.resources.head._3)
        val artifacts : Set[String] = (for (tuple <- dataset.resources) yield {tuple._1}).toSet
        for (artifact <- artifacts) {
          val fw = new FileWriter(localDir+"/"+diffId+"/"+artifact+"-diff/provenance.tsv")
          val provenanceAdds = diffVersion + " " + baseURL + "/" + dataset.name + "/" + artifact + "/" + dataset.oldVersion + "\n" +
            diffVersion + " " + baseURL + "/" + dataset.name + "/" + artifact + "/" + dataset.newVersion + "\n"
          fw.write(provenanceAdds)
          fw.close()
        }

      }
    }

    // Not tested yet
    /*

    val invoker = new DefaultInvoker

    //compress and release the diff
    "pbzip2 "+localDir+"/"+diffId+"/*/"+diffVersion+"/" !;
    val request = new DefaultInvocationRequest
    request.setPomFile(new io.File(localDir+"/"+diffId))
    request.setGoals(List("versions:set -DnewVersion="+diffVersion,"deploy").asInstanceOf[java.util.ArrayList[String]])
    invoker.execute(request)
    */
     */
  }



  private def runDiff(sourcedir:String, oldVersion:String, newVersion:String): Boolean = {
    val returncode = scriptdir+"diffscript_mod.sh -v "+diffVersion+" -d "+localDir+"/"+diffId + " "+sourcedir+" "+oldVersion+" "+newVersion !;
    if (returncode == 0) {true} else {false}
  }

  private def runRapandSort (datadir : String, logLocation : String): Boolean = {
    val returncode = scriptdir+"rapandsort_mod.sh "+datadir+" "+logLocation !;
    if (returncode == 0) {true} else {false}
  }


  private def saveDirCreation (path:String) {
    val file = new java.io.File(path)
    if (!file.isDirectory) {
      file.mkdirs
    }
  }


  private def getBaseURL (downloadURL : String): String = {
    val URLbody = downloadURL.replace("https://", "").split("/").reverse.tail.tail.tail.tail.reverse

    "https://"+URLbody.mkString("/")
  }
}

