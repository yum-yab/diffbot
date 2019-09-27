package org.dbpedia.diffbot

case class Dataset(name: String, resources : List[DBpediaArtifact], oldVersion : String, newVersion : String)

case class DBpediaFile (fileName : String, downloadURL : String)

case class DBpediaArtifact (name : String, url : String, oldResources : Iterable[DBpediaFile], newResources : Iterable[DBpediaFile])