package action

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"os"
)

func GetResultsFromBigQuery(projectId string, queryRequested string) []string {
	ctx := context.Background()
	client := createBigQueryClient(ctx, projectId)
	rows := makeQuery(ctx, client, queryRequested)
	results := parseResultsToJson(os.Stdout, rows)
	return results
}

func createBigQueryClient(ctx context.Context, projectId string) *bigquery.Client {
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()
	return client
}

func makeQuery(ctx context.Context, client *bigquery.Client, queryRequested string) (*bigquery.RowIterator) {
	log.Println("Getting data from bigQuery")
	query := client.Query(queryRequested)
	rows, err := query.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return rows
}

func parseResultsToJson(w io.Writer, it *bigquery.RowIterator) []string {
	log.Println("Parsing results to JSON")
	var results []string
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Errorf("error: %v", err)
			break
		}

		var columnNames = getColumnNames(it.Schema)
		var dataMapped = make(map[string]bigquery.Value)

		for i := 0; i < len(columnNames); i++ {
			dataMapped[columnNames[i]] = values[i]
		}
		jsonString, err := json.Marshal(dataMapped)
		if err != nil {
			fmt.Errorf("error parsing to json: %v", err)
		} else {
			results = append(results, string(jsonString))
		}
	}
	return results
}

func getColumnNames(schema bigquery.Schema) []string {
	log.Println("Getting column names from Schema")
	var columnNames = make([]string, 0)
	for i := 0; i < len(schema); i++ {
		columnNames = append(columnNames, schema[i].Name)
	}
	return columnNames
}
