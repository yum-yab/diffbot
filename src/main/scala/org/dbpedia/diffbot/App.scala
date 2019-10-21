package org.dbpedia.diffbot


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


    val diffHandler = if (config.diff.version != null && (config.diff.version match {
      case  DiffUtils.versionRegex(_) => true
      case _ => logger.info(s"Wrong version format!"); System.exit(1);false
    })) {
      new DiffHandler(config, datasets, config.diff.version)
    } else {
      new DiffHandler(config, datasets)
    }
    diffHandler.handleDatasets()


  }
}
