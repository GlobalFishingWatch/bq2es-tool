# bq2es-tool

## Description

bq2es-tool is an agnostic CLI to expose commands which allows you to import data (and other related actions) between BigQuery
and Elastic Search.

Format:
```
bq2es-tool [command] [--flags]
```

### Tech Stack:
* [Golang](https://golang.org/doc/)
* [Cobra Framework](https://github.com/spf13/cobra#working-with-flags)
* [Viper](https://github.com/spf13/viper)
* [Docker](https://docs.docker.com/)

### Git
* Repository: 
https://github.com/GlobalFishingWatch/bq2es-tool

## Usage

There are available the following commands:
* Import

---

### Command: [import]

The import command allows you to import data from BigQuery to Elastic Search. 

#### Flags
##### Required flags
- `--project-id=` the project id where we want to run the query.
- `--query=` SQL query to get rows from BigQuery.
- `--index-name=` The destination name index.
- `--elastic-search-url=` The Elasticsearch's URL. 

##### Optional flags
* `--import-mode=` The import mode allows you to define if you want to recreate the index or append the data
 | values: [ recreate | append ]. Default: recreate.
* `--on-error=` The flag allows you to define if delete all previous data or not in case an error happens. 
Values: [ delete | keep ]. Default: delete

#### Example
Here an example of this command:
```
bq2es-tool import \
  --project-id=world-fishing-827 \
  --query="SELECT * FROM scratch_megan.peru_track_data" \
  --elastic-search-url="https://gfw:****@elasticsearch.globalfishingwatch.org" \
  --index-name=test-track-data
```

When you execute this command, under the hood happens the followings steps:
* The CLI executes the SQL query and gets the rows
* The CLI parses the results from RowIterator to JSON files. The keys are the name of each column.
* The CLI imports the parsed data to Elasticsearch creating a default mapping and using the bulk method. The index's name is provided by the flag --index-name