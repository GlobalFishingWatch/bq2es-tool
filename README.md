# bq2es-tool

## Description

bq2es-tool is an agnostic CLI to expose commands which allows you to import data (and other related actions) between BigQuery
and Elastic Search services.

Format:
```
bq2es [command] [--flags]
```

### Tech Stack:
* Go
* Cobra Framework
* Docker

### Git
* Repository: 
https://github.com/GlobalFishingWatch/bq2es-tool

## Usage

There are available the following commands:
* Import
* Create-Index

---

### Command: [import]

The import command allows you to import data from BigQuery to Elastic Search. 

#### Flags
##### Required flags
- `--project-id=` (*): the project id where we want to run the query.
- `--query=` SQL query to get rows from BigQuery.
- `--index-name=` The destination name index.
- `--elastic-search-url=`: The Elasticsearch's URL. 

##### Optional flags
* `--import-mode=` The import mode allows you to define if you want to recreate the index or append the data
 | values: [ recreate | append ]. Default: recreate.
* `--on-error=` The flag allows you to define if delete all previous data or not in case an error happens. 
Values: [ delete | keep ]. Default: delete

#### Example
Here an example of this command:
```
bq2es import 
  --project-id=world-fishing-827 
  --query="SELECT * FROM scratch_megan.peru_track_data" 
  --elastic-search-url="https://global:********@elasticsearch-7.globalfishingwatch.org" 
  --index-name=test-track-data
```

When you execute this command, under the hood happens the followings steps:
* The CLI executes the SQL query and gets the rows
* The CLI parses the results from RowIterator to JSON files. The keys are the name of each column.
* The CLI imports the parsed data to Elasticsearch creating a default mapping and using the bulk method. The index's name is provided by the flag --index-name

---

### Command: [create-index]

Create a new index applying a custom mapping from Google Cloud Storage Bucket. Sometimes You can need to add a custom mapping for
an index. You need to execute this command before import the data.

#### Flags
##### Required flags
- `--bucket-name=` The source name bucket.
- `--index-name=` The destination name index.
- `--elastic-search-url=` (*): The Elasticsearch's URL. 

##### Optional flags
No optional flags.

#### Example
Here an example of this command:
```
bq2es create-index 
  --bucket-name=elastic-search-mappings
  --index-name=test-track-data
  --elastic-search-url="https://global:********@elasticsearch-7.globalfishingwatch.org" 
```

When you execute this command, under the hood happens the followings steps:
* The CLI find the JSON file in the specified bucket. Example, if you specified track-data as index-name then: `track-data.json`
* Create the index (if not exists)
* Put the mapping defined in the JSON file.