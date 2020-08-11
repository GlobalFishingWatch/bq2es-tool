package common

import (
	"github.com/elastic/go-elasticsearch/v7"
	"log"
)

func DeleteIndex(elasticSearchClient *elasticsearch.Client, indexName string) {
	log.Printf("→ ES →→ Deleting index with name %v\n", indexName)
	if _, err := elasticSearchClient.Indices.Delete([]string{indexName}); err != nil {
		log.Fatalf("→ ES →→ Cannot delete index: %s", err)
	}
}
