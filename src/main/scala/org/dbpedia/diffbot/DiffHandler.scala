package org.dbpedia.diffbot

import java.io
import java.io.{BufferedInputStream, FileInputStream, FileWriter, IOException, OutputStream, File}
import java.net.URL
import java.util.Calendar

import scala.collection.JavaConversions._
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import sys.process._
import org.apache.maven.shared.invoker._

import java.nio.file.Files.copy
import java.nio.file.Paths.get
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

import scala.collection.SortedSet
import scala.collection.mutable.ListBuffer


/**
  * Handle one Diff
  * @param datasets - List of datasets
  * @param diffVersion - version on which the diff should be released (default todays date)
  */

class DiffHandler(val config :Config, val datasets : List[Dataset] ,val diffVersion : String = DiffUtils.dateFormat.format(Calendar.getInstance().getTime())) {

  val diffId = config.diff.diffId
  // parent directory of the poms
  private val localDir = config.cnfg.localDir
  // directory with all the scripts
  private val scriptdir = config.cnfg.scriptDir
  final val logger = LoggerFactory.getLogger(this.getClass)

  private val invoker = new DefaultInvoker
  invoker.setWorkingDirectory(new io.File(s"$localDir/$diffId"))
  invoker.setMavenHome(new io.File(config.cnfg.mavenHome))




  def handleDatasets (): Unit = {
    logger.info(s"Starting the diff of ${config.diff.diffId} with version ${diffVersion}...")
    for (dataset <- datasets) {
      logger.info(s"Starting the diff of ${dataset.name}")
      for (artifact <- dataset.resources) {
        logger.info(s"Starting the diff for ${dataset.name}/${artifact.name}")
        val oldVersionPath = s"$localDir/${dataset.name}/${artifact.name}/${dataset.oldVersion}"
        val newVersionPath = s"$localDir/${dataset.name}/${artifact.name}/${dataset.newVersion}"
        val diffArtifactPath = s"$localDir/$diffId/${artifact.name}-diff"
        saveDirCreation(oldVersionPath)
        saveDirCreation(newVersionPath)
        saveDirCreation(s"$diffArtifactPath/$diffVersion")

        for (oldfile <- artifact.oldResources) {
          DiffUtils.downloadFile(new URL(oldfile.downloadURL), s"$oldVersionPath/${oldfile.fileName}")
          val newfile = getDifferentFile(artifact.newResources, oldfile)
          if ( newfile != null) {
            DiffUtils.downloadFile(new URL(newfile.downloadURL), s"$newVersionPath/${newfile.fileName}")
            logger.info(s"Starting the diff between ${oldfile.fileName} of versions ${dataset.oldVersion} and ${dataset.newVersion}...")
            diffFiles(s"$oldVersionPath/${oldfile.fileName}", s"$newVersionPath/${newfile.fileName}", s"$diffArtifactPath/$diffVersion")
          } else {
            // if the next file doesnt exist, its all deletes
            copyFile(s"$oldVersionPath/${oldfile.fileName}", s"$diffArtifactPath/$diffVersion/${insertCV(oldfile.fileName, "deletes")}")
            new File(s"$diffArtifactPath/$diffVersion/${insertCV(oldfile.fileName, "adds")}").createNewFile()
          }
        }
        for (newfile <- artifact.newResources if getDifferentFile(artifact.oldResources, newfile) == null) {
          DiffUtils.downloadFile(new URL(newfile.downloadURL), s"$newVersionPath/${newfile.fileName}")
          // if a new file is added, its all adds
          copyFile(s"$newVersionPath/${newfile.fileName}", s"$diffArtifactPath/$diffVersion/${insertCV(newfile.fileName, "adds")}")
          new File(s"$diffArtifactPath/$diffVersion/${insertCV(newfile.fileName, "deletes")}").createNewFile()
        }

        // write the provenance file
        logger.info(s"Writing the provenance for ${artifact.name}...")
        writeProvFile(s"${artifact.url}/${dataset.oldVersion}", s"${artifact.url}/${dataset.newVersion}", diffArtifactPath)
      }
      logger.info(s"Successfully finished ${dataset.name}!")
    }


    //change the version of the pom file
    runMavenCommand(new io.File(s"$localDir/$diffId/pom.xml"), List(s"versions:set -DnewVersion=$diffVersion"))

    // deploy the release (config for that in the pom file)
    runMavenCommand(new io.File(s"$localDir/$diffId/pom.xml"), List("validate"))
    logger.info(s"Finished all datasets and deploying to the databus")
  }


  private def diffFiles(oldFilePath : String, newFilePath : String, target : String): Boolean = {
    val returncode = s"$scriptdir/diff2Files.sh $oldFilePath $newFilePath $target" !;
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
    // Removes 4 last parts of URL: filename, version, artifact and groupId
    val URLbody = downloadURL.replace("https://", "").split("/").reverse.tail.tail.tail.tail.reverse

    "https://"+URLbody.mkString("/")
  }

  private def writeProvFile (oldVersionURL : String, newVersionURL : String, path : String): Unit = {
    val fw = new FileWriter(s"$path/provenance.tsv", true)
    val provenanceAdds = s"$diffVersion\t$oldVersionURL\n" +
      s"$diffVersion\t$newVersionURL\n"
    fw.write(provenanceAdds)
    fw.close()
  }

  private def runMavenCommand (pomfile : io.File, goals : List[String]): Unit = {
    val request = new DefaultInvocationRequest
    request.setPomFile(pomfile)
    request.setGoals(goals)
    invoker.execute(request)
  }

  private def getDifferentFile(list : Iterable[DBpediaFile], file : DBpediaFile): DBpediaFile = {
    for (f <- list if f.fileName == file.fileName) return f
    null
  }

  private def copyFile (sourcePath : String, targetPath : String): Unit = {
    val src = get(sourcePath)
    val target = get(targetPath)

    copy(src,target,REPLACE_EXISTING)
  }

  private def insertCV (filename : String, cv : String): String = {
    val filenamePattern = """([\w]+)(_[\w=]+)*?(\.[\w]+)(\.[\w]+)?""".r

    filename match {
      case filenamePattern(artifact, cvs, fileType, compression) => s"$artifact${cvs}_$cv$fileType$compression"
    }
  }
}

