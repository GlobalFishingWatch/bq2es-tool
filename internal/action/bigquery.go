package action

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"google.golang.org/api/iterator"
	"log"
)

func GetResultsFromBigQuery(projectId string, queryRequested string) [][]byte {
	ctx := context.Background()
	client := createBigQueryClient(ctx, projectId)
	rows := makeQuery(ctx, client, queryRequested)
	results := parseResultsToJson(rows)
	return results
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
	log.Println("→ BQ →→ Getting data from bigQuery")
	query := client.Query(queryRequested)
	rows, err := query.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return rows
}

func parseResultsToJson(it *bigquery.RowIterator) [][]byte {
	log.Println("→ BQ →→ Parsing results to JSON")
	var columnNames = getColumnNames(it.Schema)
	var results [][]byte
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
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

		results = append(results, jsonString)
	}
	return results
}

func getColumnNames(schema bigquery.Schema) []string {
	log.Println("→ BQ →→ Getting column's names from Schema")
	var columnNames = make([]string, 0)
	for i := 0; i < len(schema); i++ {
		columnNames = append(columnNames, schema[i].Name)
	}
	return columnNames
}
