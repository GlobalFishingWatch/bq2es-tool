package action

import (
	"bq2es/internal/common"
	"bq2es/internal/utils"
	"bq2es/types"
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
	"strings"
	"time"
)

var elasticSearchUrl string
var elasticSearchClient *elasticsearch.Client
var onErrorAction string

func ImportBigQueryToElasticSearch(query string, url string, projectId string, indexName string, importMode string, onError string) {

	validateFlags(url, importMode, onError)

	elasticSearchUrl = url
	onErrorAction = onError
	elasticSearchClient = common.CreateElasticSearchClient(url)
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
	bigQueryClient := common.CreateBigQueryClient(ctx, projectId)
	iterator := makeQuery(ctx, bigQueryClient, queryRequested)
	go parseResultsToJson(iterator, ch)
}

func makeQuery(ctx context.Context, bigQueryClient *bigquery.Client, queryRequested string) (*bigquery.RowIterator) {
	log.Println("→ BQ →→ Making query to get data from bigQuery")
	query := bigQueryClient.Query(queryRequested)
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("→ BQ →→ Error counting rows: %v", err)
	}
	return it
}

func parseResultsToJson(it *bigquery.RowIterator, ch chan []byte) {
	log.Println("→ BQ →→ Parsing results to JSON")
	var columnNames = getColumnNames(it.Schema)
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

		var dataMapped = make(map[string]bigquery.Value)

		for i := 0; i < len(columnNames); i++ {
			dataMapped[columnNames[i]] = values[i]
		}

		jsonString, err := json.Marshal(dataMapped)
		if err != nil {
			log.Fatalf("→ BQ →→ Error parsing to json: %v", err)
		}
		ch <- jsonString
	}
}

func getColumnNames(schema bigquery.Schema) []string {
	log.Println("→ BQ →→ Getting column's names from Schema")
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
		raw map[string]interface{}
		blk *types.BulkResponse
		numErrors int
		numItems int
		numIndexed int
	)

	log.Printf("Batch [%d]", currentBatch)

	res, err = elasticSearchClient.Bulk(bytes.NewReader(buf.Bytes()), elasticSearchClient.Bulk.WithIndex(indexName))
	if err != nil {
		if onErrorAction == "delete" {
			common.DeleteIndex(elasticSearchClient, indexName)
		}
		log.Fatalf("Failure indexing Batch %d: %s", currentBatch, err)
	}

	if res.IsError() {
		numErrors += numItems
		if onErrorAction == "delete" {
			common.DeleteIndex(elasticSearchClient, indexName)
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
			common.DeleteIndex(elasticSearchClient, indexName)
		}
		log.Fatalf("Failure to to parse response body: %s", err)
	}

	for _, d := range blk.Items {
		if d.Index.Status > 201 {
			numErrors++
			if onErrorAction == "delete" {
				common.DeleteIndex(elasticSearchClient, indexName)
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

func recreateIndex(indexName string) {
	log.Printf("→ ES →→ Recreating index with name %v\n", indexName)
	common.DeleteIndex(elasticSearchClient, indexName)
	common.CreateIndex(elasticSearchClient, indexName)
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
