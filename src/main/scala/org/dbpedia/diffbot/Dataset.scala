package org.dbpedia.diffbot

class Dataset(val name: String, val resources : List[(String,String,String)]) {

  //Old and New version generated from the sorted resources list
  final val oldVersion = resources.head._2
  final val newVersion = (for (tuple <- resources if (tuple._2 != oldVersion)) yield {tuple._2}).head

}
