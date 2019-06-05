# DBpedia-Diffbot

A bot for chronically diffing some supervised datasets.

## Usage

### Configuring the diffbot

* datasets: configuration for the in the diff included datasets
	- supervised: Array of supervised datasets (must be released by the same releaser)
	- endpoint: related SPARQL-Endpoint
	- releaser: identifier of the releaser (e.g dbpedia)

* diff: Diff related configuration
	- diffId: name of the diff
	- endpoint: related SPARQL-Endpoint
	- releaser: identifier of the releaser (in this case releaser of all the diffs, propably you)
	- version: if this param is set, the diff version is this, else its todays date

* cnfg: system related configuration
	- mavenHome: your maven home (run mvn --version to see it)
	- localDir: the directory where the datasets and the diffs are saved (must be big enough)
	- scripts: directory with the realted scripts (default ./src/main/resources, dont change if you didn´t change the scripts)
