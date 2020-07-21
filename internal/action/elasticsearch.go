package action

import (
	"bq2es/types"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
	"strings"
	"time"
)

func ImportBulk(address string, documents [][]byte, indexName string, importMode string) {

	const Batch = 1000

	var (
		buf bytes.Buffer
		res *esapi.Response
		err error
		raw map[string]interface{}
		blk *types.BulkResponse

		numItems   int
		numErrors  int
		numIndexed int
		numBatches int
		currBatch  int
	)

	numOfDocuments := len(documents)
	start := time.Now().UTC()

	es := getElasticClient(address)

	numBatches = calculateTotalBatches(numOfDocuments, Batch)
	createPreReport(numOfDocuments, Batch, numBatches, start)

	if strings.TrimRight(importMode, "\n") == "recreate" {
		recreateIndex(es, indexName, res, err)
	}

	for i, doc := range documents {
		numItems++

		currBatch = i / Batch
		if i == numOfDocuments - 1 {
			currBatch++
		}

		preparePayload(&buf, doc)

		if i > 0 && i%Batch == 0 || i == numOfDocuments - 1 {
			fmt.Printf("[%d/%d] ", currBatch, numBatches)

			res, err = es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex(indexName))
			if err != nil {
				log.Fatalf("Failure indexing Batch %d: %s", currBatch, err)
			}

			if res.IsError() {
				numErrors += numItems
				if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
					continue
				}
				log.Printf("  Error: [%d] %s: %s",
					res.StatusCode,
					raw["error"].(map[string]interface{})["type"],
					raw["error"].(map[string]interface{})["reason"],
				)
				continue
			}

			if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
				log.Fatalf("Failure to to parse response body: %s", err)
				continue
			}

			for _, d := range blk.Items {
				if d.Index.Status > 201 {
					numErrors++
					log.Printf("  Error: [%d]: %s: %s: %s: %s",
						d.Index.Status,
						d.Index.Error.Type,
						d.Index.Error.Reason,
						d.Index.Error.Cause.Type,
						d.Index.Error.Cause.Reason,
					)
					continue
				}
				numIndexed++
			}
			res.Body.Close()
			buf.Reset()
			numItems = 0
		}
	}
	createReport(start, numErrors, numIndexed)
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

func recreateIndex(es *elasticsearch.Client, indexName string, res *esapi.Response, err error) {
	log.Printf("→ ES →→ Recreating index with name %v\n", indexName)
	if res, err = es.Indices.Delete([]string{indexName}); err != nil {
		log.Fatalf("→ ES →→ Cannot delete index: %s", err)
	}
	res, err = es.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("→ ES →→ Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot create index: %s", res)
	}
}

func calculateTotalBatches(count int, Batch int) int {
	if count%Batch == 0 {
		return count / Batch
	}
	return (count / Batch) + 1
}

func preparePayload(buf *bytes.Buffer, document []byte) {
	meta := []byte(fmt.Sprintf(`{ "index" : {  }%s`,"\n"))
	document = append(document, "\n"...)
	buf.Grow(len(meta) + len(document))
	buf.Write(meta)
	buf.Write(document)
}

func createPreReport(numOfDocuments int, Batch int, numBatches int, start time.Time) {
	log.Printf(
		"→ ES →→ \x1b[1mBulk\x1b[0m: documents [%s] Batch size [%s]",
		humanize.Comma(int64(numOfDocuments)), humanize.Comma(int64(Batch)))
	log.Printf("→ ES →→  Number of Batchs %v", numBatches)
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
