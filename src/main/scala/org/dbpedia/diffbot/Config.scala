package org.dbpedia.diffbot

case class Config(diff : DiffConfig, dataset : DatasetConfig, cnfg : LocalConfig)

case class DiffConfig(diffId : String, endpoint : String, releaser : String, version : String)

case class DatasetConfig(datasetNames : List[String], endpoint :String, releaser : String)

case class LocalConfig(mavenHome : String, localDir : String)
