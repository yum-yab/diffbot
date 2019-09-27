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
