package action

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"github.com/GlobalFishingWatch/bq2es-tool/internal/common"
	"github.com/GlobalFishingWatch/bq2es-tool/types"
	"github.com/GlobalFishingWatch/bq2es-tool/utils"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"google.golang.org/api/iterator"
	"log"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var esClient *elasticsearch.Client
var bqClient *bigquery.Client

var onErrorAction string
var temporalIndexName string

func ImportBigQueryToElasticSearch(params types.ImportParams) {

	validateFlags(params)

	ctx := context.Background()
	esClient = common.CreateElasticSearchClient(params.ElasticSearchUrl)
	bqClient = common.CreateBigQueryClient(ctx, params.ProjectId)

	onErrorAction = params.OnError

	indexExists := common.CheckIfIndexExists(esClient, params.IndexName)
	if indexExists == true && onErrorAction == "reindex" {
		log.Println("→ Reindexing index to avoid losing data")
		temporalIndexName = params.IndexName  + "-" + time.Now().UTC().Format("2006-01-02") + "-reindexed"
		reindex(params.IndexName, temporalIndexName)
	}

	ch := make(chan  map[string]bigquery.Value, 100)

	log.Println("→ Getting results from big query")
	getResultsFromBigQuery(ctx, params.Query, ch)


	log.Println("→ Importing results to elasticsearch (Bulk)")
	importBulk(params.IndexName, params.ImportMode, params.Normalize, params.NormalizeEndpoint, ch)
}

func validateFlags(params types.ImportParams) {

	utils.ValidateUrl(params.ElasticSearchUrl)

	if strings.TrimRight(params.ImportMode, "\n") != "recreate" && strings.TrimRight(params.ImportMode, "\n") != "append" {
		log.Fatalln("--import-mode should equal to 'recreate' or 'append'")
	}
	if strings.TrimRight(params.OnError, "\n") != "delete" && strings.TrimRight(params.OnError, "\n") != "keep"  && strings.TrimRight(params.OnError, "\n") != "reindex" {
		log.Fatalln("--on-error should equal to 'delete', 'keep' or 'reindex'")
	}

	if strings.TrimRight(params.Normalize, "\n") != "" && strings.TrimRight(params.NormalizeEndpoint, "\n") == "" {
		log.Fatalln("if you set the flag normalized, you must to set the normalize endpoint")
	}

}


// BigQuery Functions
func getResultsFromBigQuery(ctx context.Context, queryRequested string, ch chan map[string]bigquery.Value) {
	iterator := makeQuery(ctx, queryRequested)
	go parseResultsToJson(iterator, ch)
}

func makeQuery(ctx context.Context, queryRequested string) (*bigquery.RowIterator) {
	log.Println("→ BQ →→ Making query to get data from bigQuery")
	query := bqClient.Query(queryRequested)
	query.AllowLargeResults = true
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("→ BQ →→ Error counting rows: %v", err)
	}
	return it
}

func parseResultsToJson(it *bigquery.RowIterator, ch chan  map[string]bigquery.Value) {
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

		ch <- dataMapped
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


// Elastic Search Functions
func importBulk(indexName string, importMode string, normalize string, normalizeEndpoint string, ch chan map[string]bigquery.Value) {
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
		if strings.TrimRight(normalize, "\n") != "" {
			if doc[normalize] == nil {
				log.Printf("The property %v does not exist on the documents", normalize)
				doc["normalized_" + normalize] = ""
			} else {
				value := strings.ReplaceAll(doc[normalize].(string), `\`, `\\`)
				value = strings.ReplaceAll(value, `"`, `\"`)
				var requestBody = `{"type": "` + normalize +`", "value": "` + value + `"}`
				log.Println(requestBody)
				var jsonStr = []byte(requestBody)
				req, err := http.NewRequest("POST", normalizeEndpoint, bytes.NewBuffer(jsonStr))
				req.Header.Set("Content-Type", "application/json")
				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					log.Fatalf("Error normalizing property %s: %s", normalize, err)
				}
				defer resp.Body.Close()

				if resp.StatusCode == 500 {
					doc["normalized_" + normalize] = ""
				} else if resp.StatusCode != 200 {
					log.Fatalf("Error normalizing the property %s. Error: %s", normalize, resp.Status)
				} else {
					var responseParsed = types.NormalizeResponse{}
					err = json.NewDecoder(resp.Body).Decode(&responseParsed)
					if err != nil {
						log.Fatalf("Error normalizing property %s: %s", normalize, err)
					}
					doc["normalized_" + normalize] = responseParsed.Result
				}
			}
		}
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


	if numItems > 0 {
		currentBatch ++
		errors, items, indexed := executeBulk(currentBatch, indexName, buf)
		numErrors += errors
		numItems += items
		numIndexed += indexed
		buf.Reset()
	}

	createReport(start, numErrors, numIndexed)
}

func executeBulk(currentBatch int, indexName string, buf bytes.Buffer) (int, int, int) {
	var (
		raw map[string]interface{}
		blk *types.BulkResponse
		numErrors int
		numItems int
		numIndexed int
	)

	log.Printf("Batch [%d]", currentBatch)

	res, err := esClient.Bulk(bytes.NewReader(buf.Bytes()), esClient.Bulk.WithIndex(indexName))
	if err != nil {
		executeOnErrorAction(indexName)
		log.Fatalf("Failure indexing Batch %d: %s", currentBatch, err)
	}

	if res.IsError() {
		numErrors += numItems
		executeOnErrorAction(indexName)
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
		executeOnErrorAction(indexName)
		log.Fatalf("Failure to to parse response body: %s", err)
	}

	for _, d := range blk.Items {
		if d.Index.Status > 201 {
			numErrors++
			executeOnErrorAction(indexName)
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

func executeOnErrorAction(indexName string) {
	if onErrorAction == "delete" {
		common.DeleteIndex(esClient, indexName)
	}
	if onErrorAction == "reindex" {
		common.DeleteIndex(esClient, indexName)
		reindex(temporalIndexName, indexName)
		common.DeleteIndex(esClient, temporalIndexName)
	}
}

func recreateIndex(indexName string) {
	log.Printf("→ ES →→ Recreating index with name %v\n", indexName)
	common.DeleteIndex(esClient, indexName)
	res, err := esClient.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("→ ES →→ Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot create index: %s", res)
	}
}

func reindex(sourceIndexName string, destinationIndexName string) {
	existsDestinationIndex := common.CheckIfIndexExists(esClient, destinationIndexName)
	if existsDestinationIndex == true {
		common.DeleteIndex(esClient, destinationIndexName)
	}

	log.Printf("→ ES →→ Reindexing from %s to %s\n", sourceIndexName, destinationIndexName)
	reindexBody := map[string]map[string]string{
		"source": {"index": sourceIndexName},
		"dest": {"index": destinationIndexName},
	}
	body, err := json.Marshal(reindexBody)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating body to reindex %s", err)
	}

	res, err := esClient.Reindex(bytes.NewReader(body), func(request *esapi.ReindexRequest) {
		waitForCompletion := false
		request.WaitForCompletion = &waitForCompletion
	})
	if err != nil {
		log.Fatalf("→ ES →→ Error requesting reindex %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot reindex: %s", res)
	}

	responseBody := utils.ParseEsAPIResponse(res)
	taskId := responseBody["task"].(string)
	log.Printf("→ ES →→ Reindex process started async. Task id: %s \n", taskId)

	for {
		res, err := esClient.Tasks.Get(taskId)
		if err != nil {
			log.Fatalf("→ ES →→ Error requesting reindex %s", err)
		}
		if res.IsError() {
			log.Fatalf("→ ES →→ Cannot reindex: %s", res)
		}
		responseBody = utils.ParseEsAPIResponse(res)
		taskStatus := responseBody["completed"].(bool)
		if taskStatus == true {
			break
		}
		time.Sleep(5000 * time.Millisecond)
	}
	log.Println("→ ES →→ Reindex process completed")
	common.DeleteIndex(esClient, sourceIndexName)
}

func preparePayload(buf *bytes.Buffer, document map[string]bigquery.Value) {
	var meta []byte
	if _, found := document["id"]; found {
		meta = []byte(fmt.Sprintf(`{ "index" : { "_id": "%s" }}%s`,document["id"].(string),"\n"))
	} else {
		meta = []byte(fmt.Sprintf(`{ "index" : { }%s`, "\n"))
	}

	body, err := json.Marshal(document)
	if err != nil {
		log.Fatalf("→ ES →→ Error parsing to json: %v", err)
	}
	body = append(body, "\n"...)
	buf.Grow(len(meta) + len(body))
	buf.Write(meta)
	buf.Write(body)
}

// Reports functions
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
