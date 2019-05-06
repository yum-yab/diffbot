package org.dbpedia.diffbot

import java.io
import java.io.FileWriter

import scala.reflect.io.{Directory, File}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import sys.process._
import org.apache.maven.shared.invoker._

/**
  * Handle one Dataset
  * @param dataset
  * @param diffVersion
  * @param diffId
  */

class DatasetHandler(val dataset : Dataset,val diffVersion : String, val diffId : String) {

  // directory with all the scripts
  private val localDir = DiffUtils.readStringFromConfig("cnfg.localDir")
  private val scriptdir = ConfigFactory.load("diffbot.conf").getString("cnfg.scripts")
  final val logger = LoggerFactory.getLogger(this.getClass)


  def handleDataset(): Unit = {
    //TODO: Write this method
    //handle artifact

    val versionsList = DiffUtils.getAllArtifactVersions(dataset.artifacts(0), dataset.name).toList.sortBy(tuple => tuple._1).reverse
    val newVersion = versionsList.head
    val oldVersion = versionsList.tail.head

    val provenanceAdds = (for (artifact <- dataset.artifacts) yield (artifact -> handleArtifact(artifact))).toMap

    //run the diff
    if (!runDiff(localDir+"/"+dataset.name, oldVersion._1, newVersion._1)) {
      logger.error(printf("There was an error in diffing the Versions %o and %y.",oldVersion._1,newVersion._1).toString)
      System.exit(1)
    }
    // Add values to the provenance
    for (artifact <- provenanceAdds.keys) {
      val fw = new FileWriter(localDir+"/"+diffId+"/"+artifact+"/provenance.tsv")
      fw.write(provenanceAdds(artifact))
      fw.close()
    }


    val invoker = new DefaultInvoker
    //compress & release the rappered data
    "pbzip2 "+localDir+"/"+dataset.name+"/*/"+oldVersion+"/*.*" !;
    "pbzip2 "+localDir+"/"+dataset.name+"/*/"+newVersion+"/*.*" !;

    for (version <- List(oldVersion._1, newVersion._1)) {
      val request = new DefaultInvocationRequest
      request.setPomFile(new java.io.File(localDir+"/"+dataset.name+"/pom.xml"))
      request.setGoals(List("versions:set -DnewVersion="+version,"deploy").asInstanceOf[java.util.ArrayList[String]])
      invoker.execute(request)
    }
    //compress and release the diff
    "pbzip2 "+localDir+"/"+diffId+"/*/"+diffVersion !;
    val request = new DefaultInvocationRequest
    request.setPomFile(new io.File(localDir+"/"+diffId))
    request.setGoals(List("versions:set -DnewVersion="+diffVersion,"deploy").asInstanceOf[java.util.ArrayList[String]])
    invoker.execute(request)
  }

  /**
    * Downloads the Files and rapandsorts them
    * @param artifact
    * @return The String for the provenance script
    */

  private def handleArtifact(artifact : String): String ={
    //Check for the right versions to be downloaded

    val versionsList = DiffUtils.getAllArtifactVersions(artifact, dataset.name).toList.sortBy(tuple => tuple._1).reverse
    val newVersion = versionsList.head
    val oldVersion = versionsList.tail.head



    val oldVersionDir = localDir+"/"+dataset.name+"/"+artifact+"/"+oldVersion._1
    val newVersionDir = localDir+"/"+dataset.name+"/"+artifact+"/"+newVersion._1


    //create the direcory structure
    saveDirCreation(localDir+"/"+dataset.name+"/")
    saveDirCreation(localDir+"/"+dataset.name+"/"+artifact)
    saveDirCreation(oldVersionDir)
    saveDirCreation(newVersionDir)

    val provstring = diffVersion + " "+ oldVersion._2.replace("dataid.ttl", "") + "\n"+
      diffVersion + " " + newVersion._2.replace("dataid.ttl", "")

    // Download the needed files
    //newVersion
    DiffUtils.downloadFiles(DiffUtils.getDownloadURLS(newVersion._2), newVersionDir)
    //oldVersion
    DiffUtils.downloadFiles(DiffUtils.getDownloadURLS(oldVersion._2), oldVersionDir)

    //rapandsort them
    for {versionDir <- List(oldVersionDir,newVersionDir)}
      if (!runRapandSort(versionDir, localDir)) {
        logger.warn(printf("There was a problem in RapAndSorting version %v of %a.",versionDir.split("/").reverse.head, artifact).toString)
      }

  }

  private def runDiff(sourcedir:String, oldVersion:String, newVersion:String): Boolean = {
    val returncode = scriptdir+"diffscript_mod.sh -v "+diffVersion+" -d "+localDir+"/"+diffId + " "+sourcedir+" "+oldVersion+" "+newVersion
    if (returncode == 0) {true} else {false}
  }

  private def runRapandSort (datadir : String, logLocation : String): Boolean = {
    val returncode = scriptdir+"rapandsort_mod.sh "+datadir+" "+logLocation !;
    if (returncode == 0) {true} else {false}
  }


  private def saveDirCreation (path:String) {
    val file = new java.io.File(path)
    if (!file.isDirectory) {
      file.mkdir
    }
  }
}

