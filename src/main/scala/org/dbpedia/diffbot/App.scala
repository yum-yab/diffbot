package org.dbpedia.diffbot

import java.io.File

import org.slf4j.LoggerFactory

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val config = DiffUtils.generateConfig("diffbot.conf")


    logger.info(s"datasets: ${config.dataset}")
    logger.info(s"Local: ${config.cnfg}")
    logger.info(s"diff: ${config.diff}")

    val generator = new DatasetGenerator(config)

    val datasets = generator.generateConfigDatasets()


    val diffHandler = if (config.diff.version != null) {
      new DiffHandler(config, datasets, config.diff.version)
    } else {
      new DiffHandler(config, datasets)
    }
    diffHandler.handleDatasets()
  }



}
