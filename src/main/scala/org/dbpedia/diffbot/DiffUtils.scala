package org.dbpedia.diffbot

import java.io.{File}

import com.typesafe.config.ConfigFactory
import org.slf4j.{LoggerFactory}

import sys.process._
import java.net.{HttpURLConnection, URL}
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._

object DiffUtils {

  final val logger = LoggerFactory.getLogger(this.getClass)
  final val dateFormat = new SimpleDateFormat("yyyy.MM.dd")

  def readStringFromConfig (id : String, conf : com.typesafe.config.Config): String = {
    conf.getString(id)
  }

  def generateConfig(configPath : String): Config = {
    val config = ConfigFactory.load(configPath)
    val diffConf = DiffConfig(
      diffId = config.getString("diff.diffId"),
      endpoint = config.getString("diff.endpoint"),
      releaser = config.getString("diff.releaser"),
      version = try config.getString("diff.version") catch {case _ : Throwable => null}
    )
    val datasetConf = DatasetConfig(
      datasetNames = config.getStringList("datasets.supervised").asScala.toList,
      endpoint = config.getString("datasets.endpoint"),
      releaser = config.getString("datasets.releaser")
    )
    val localConf = LocalConfig(
      mavenHome = config.getString("cnfg.mavenHome"),
      localDir = config.getString("cnfg.localDir"),
      scriptDir = config.getString("cnfg.scripts")
    )
    Config(
      diff = diffConf,
      dataset = datasetConf,
      cnfg = localConf
    )
  }


  /**
    * Downloads a File to the given Path (copied from StackOverflow: https://stackoverflow.com/questions/24162478/how-to-download-and-save-a-file-from-the-internet-using-scala)
    * @param url File-URL
    * @param path Path to save to
    * @return False if an Error occurred, else true
    */

  def downloadFile (url : URL, path : String) : Unit = {
    val file = new File(path)
    if (!file.exists()) {
      logger.info(s"No local file found, proceed to download it...")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(5000)
      connection.connect()

      if (connection.getResponseCode >= 400) {
        logger.error(s"There was a problem downloading the File from $url to $path, code ${connection.getResponseCode}.")
      } else {
        try {
          url #> file !!;
          logger.info(s"Sucessfully downloaded to $path")
        } catch {
          case e: Exception => logger.error(s"There was a problem saving the File to $path \nStacktrace: ${e.getMessage}");
        }
      }
    } else {
      logger.info(s"File $file existing! No need to download...")
    }
  }

  def getIdentifier (url : String): String = {
    url.split("/").reverse.head
  }
}

