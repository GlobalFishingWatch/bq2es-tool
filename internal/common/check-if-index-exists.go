package common

import (
	"github.com/elastic/go-elasticsearch/v7"
	"log"
)

func CheckIfIndexExists (esClient *elasticsearch.Client, indexName string) bool {
	res, err := esClient.Indices.Exists([]string{indexName})
	if err != nil {
		log.Fatalf("→ ES →→ Cannot check if index exists: %s", err)
	}
	log.Println("→ ES →→ Checking if index exists on ElasticSearch: ", res.StatusCode == 200)
	return res.StatusCode == 200
}