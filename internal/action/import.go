package action

import (
	"bq2es/types"
	"bq2es/utils"
	"bytes"
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"google.golang.org/api/iterator"
	"log"
	"reflect"
	"strings"
	"time"
)

var elasticUrl string
var onErrorAction string

func ImportBigQueryToElasticSearch(query string, url string, projectId string, indexName string, importMode string, onError string) {

	validateFlags(url, importMode, onError)

	elasticUrl = url
	onErrorAction = onError
	ch := make(chan []byte, 100)

	log.Println("→ Getting results from big query")
	getResultsFromBigQuery(projectId, query, ch)

	log.Println("→ Importing results to elasticsearch (Bulk)")
	importBulk(indexName, importMode, ch)
}

func validateFlags(url string, importMode string, onError string) {

	utils.ValidateUrl(url)

	if strings.TrimRight(importMode, "\n") != "recreate" && strings.TrimRight(importMode, "\n") != "append" {
		log.Fatalln("--import-mode should equal to 'recreate' or 'append'")
	}
	if strings.TrimRight(onError, "\n") != "delete" && strings.TrimRight(onError, "\n") != "keep" {
		log.Fatalln("--on-error should equal to 'delete' or 'keep'")
	}
}

func getResultsFromBigQuery(projectId string, queryRequested string, ch chan []byte) {
	ctx := context.Background()
	client := createBigQueryClient(ctx, projectId)
	iterator := makeQuery(ctx, client, queryRequested)
	go parseResultsToJson(iterator, ch)
}

func createBigQueryClient(ctx context.Context, projectId string) *bigquery.Client {
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("→ BQ →→ bigquery.NewClient: %v", err)
	}
	defer client.Close()
	return client
}

func makeQuery(ctx context.Context, client *bigquery.Client, queryRequested string) (*bigquery.RowIterator) {
	log.Println("→ BQ →→ Making query to get data from bigQuery")
	query := client.Query(queryRequested)
	query.AllowLargeResults = true
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("→ BQ →→ Error counting rows: %v", err)
	}
	return it
}

func parseResultsToJson(it *bigquery.RowIterator, ch chan []byte) {
	log.Println("→ BQ →→ Parsing results to JSON")

	for {
		var values []bigquery.Value
		err := it.Next(&values)

		if err == iterator.Done {
			close(ch)
			break
		}
		if err != nil {
			log.Fatalf("→ BQ →→ Error: %v", err)
		}

		var dataMapped = toMapJson(values, it.Schema)

		jsonString, err := json.Marshal(dataMapped)
		if err != nil {
			log.Fatalf("→ BQ →→ Error parsing to json: %v", err)
		}
		ch <- jsonString
	}
}

func toMapJson (values []bigquery.Value, schema bigquery.Schema) map[string]bigquery.Value {
	var columnNames = getColumnNames(schema)
	var dataMapped = make(map[string]bigquery.Value)
	for i := 0; i < len(columnNames); i++ {
		if schema[i].Type == "RECORD" {
			if values[i] == nil {
				dataMapped[columnNames[i]] = values[i]
				continue
			}
			valuesNested := values[i].([]bigquery.Value)
			var valuesParsed = make([]map[string]bigquery.Value, len(valuesNested))
			var aux = make(map[string]bigquery.Value)
			for c := 0; c < len(valuesNested); c++ {
				if reflect.TypeOf(valuesNested[c]).Kind() != reflect.Interface &&
					reflect.TypeOf(valuesNested[c]).Kind() != reflect.Slice {
					var columnNamesNested = getColumnNames(schema[i].Schema)
					aux[columnNamesNested[c]] = valuesNested[c]
					dataMapped[columnNames[i]] = aux
				} else {
					valuesParsed[c] = toMapJsonNested(valuesNested[c].([]bigquery.Value), schema[i].Schema)
					dataMapped[columnNames[i]] = valuesParsed
				}
			}
		} else {
			dataMapped[columnNames[i]] = values[i]
		}
	}
	return dataMapped
}

func toMapJsonNested (value []bigquery.Value, schema bigquery.Schema) map[string]bigquery.Value {
	var columnNames = getColumnNames(schema)
	var dataMapped = make(map[string]bigquery.Value)
	for c := 0; c < len(columnNames); c++ {
		dataMapped[columnNames[c]] = value[c]
	}
	return dataMapped
}

func getColumnNames(schema bigquery.Schema) []string {
	var columnNames = make([]string, 0)
	for i := 0; i < len(schema); i++ {
		columnNames = append(columnNames, schema[i].Name)
	}
	return columnNames
}

func importBulk(indexName string, importMode string, ch chan []byte) {
	log.Println("→ ES →→ Importing data to ElasticSearch")

	const Batch = 1000

	var (
		buf bytes.Buffer
		numItems   int
		numErrors  int
		numIndexed int
		currentBatch  int
	)

	start := time.Now().UTC()

	createPreReport(Batch, start)

	if strings.TrimRight(importMode, "\n") == "recreate" {
		recreateIndex(indexName)
	}

	numItems = 0
	currentBatch = 0
	for doc := range ch {
		preparePayload(&buf, doc)
		numItems ++
		if numItems == Batch {
			currentBatch ++
			errors, items, indexed := executeBulk(currentBatch, indexName, buf)
			numErrors += errors
			numItems += items
			numIndexed += indexed
			buf.Reset()
			numItems = 0
		}
	}

	if numItems <= Batch {
		currentBatch ++
		errors, items, indexed := executeBulk(currentBatch, indexName, buf)
		numErrors += errors
		numItems += items
		numIndexed += indexed
	}
	createReport(start, numErrors, numIndexed)
}

func executeBulk(currentBatch int, indexName string, buf bytes.Buffer) (int, int, int) {
	var (
		res *esapi.Response
		err error
		es *elasticsearch.Client
		raw map[string]interface{}
		blk *types.BulkResponse
		numErrors int
		numItems int
		numIndexed int
	)

	es = getElasticClient(elasticUrl)
	log.Printf("Batch [%d]", currentBatch)

	res, err = es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex(indexName))
	if err != nil {
		if onErrorAction == "delete" {
			deleteIndex(indexName)
		}
		log.Fatalf("Failure indexing Batch %d: %s", currentBatch, err)
	}

	if res.IsError() {
		numErrors += numItems
		if onErrorAction == "delete" {
			deleteIndex(indexName)
		}
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		}
		log.Fatalf("  Error: [%d] %s: %s",
			res.StatusCode,
			raw["error"].(map[string]interface{})["type"],
			raw["error"].(map[string]interface{})["reason"],
		)
	}

	if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
		if onErrorAction == "delete" {
			deleteIndex(indexName)
		}
		log.Fatalf("Failure to to parse response body: %s", err)
	}

	for _, d := range blk.Items {
		if d.Index.Status > 201 {
			numErrors++
			if onErrorAction == "delete" {
				deleteIndex(indexName)
			}
			log.Fatalf("  Error: [%d]: %s: %s: %s: %s",
				d.Index.Status,
				d.Index.Error.Type,
				d.Index.Error.Reason,
				d.Index.Error.Cause.Type,
				d.Index.Error.Cause.Reason,
			)
		}
		numIndexed++
	}
	res.Body.Close()
	return numErrors, numItems, numIndexed
}

func getElasticClient(address string) *elasticsearch.Client {
	cfg := elasticsearch.Config{
		Addresses: []string{address},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating the client: %s", err)
	}
	return es
}

func recreateIndex(indexName string) {

	var (
		res *esapi.Response
		err error
		es *elasticsearch.Client
	)

	es = getElasticClient(elasticUrl)
	log.Printf("→ ES →→ Recreating index with name %v\n", indexName)
	deleteIndex(indexName)
	res, err = es.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("→ ES →→ Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot create index: %s", res)
	}
}

func deleteIndex(indexName string) {
	var (
		err error
		es *elasticsearch.Client
	)

	es = getElasticClient(elasticUrl)
	log.Printf("→ ES →→ Deleting index with name %v\n", indexName)
	if _, err = es.Indices.Delete([]string{indexName}); err != nil {
		log.Fatalf("→ ES →→ Cannot delete index: %s", err)
	}
}


func preparePayload(buf *bytes.Buffer, document []byte) {
	meta := []byte(fmt.Sprintf(`{ "index" : {  }%s`,"\n"))
	document = append(document, "\n"...)
	buf.Grow(len(meta) + len(document))
	buf.Write(meta)
	buf.Write(document)
}

func createPreReport(Batch int, start time.Time) {
	log.Printf(
		"→ ES →→ \x1b[1mBulk\x1b[0m: Batch size [%s]",
		humanize.Comma(int64(Batch)))
	log.Printf("→ ES →→  Start time: %v\n", start)
	log.Print("→ ES →→  Sending Batch ")
	log.Println(strings.Repeat("▁", 65))
}

func createReport(start time.Time, numErrors int, numIndexed int) {
	log.Print("\n")
	log.Println(strings.Repeat("▔", 65))

	duration := time.Since(start)

	if numErrors > 0 {
		log.Fatalf(
			"→ ES →→ Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			humanize.Comma(int64(numErrors)),
			duration.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(duration/time.Millisecond)*float64(numIndexed))),
		)
		return
	}
	log.Printf(
		"→ ES →→ Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
		humanize.Comma(int64(numIndexed)),
		duration.Truncate(time.Millisecond),
		humanize.Comma(int64(1000.0/float64(duration/time.Millisecond)*float64(numIndexed))),
	)
}
