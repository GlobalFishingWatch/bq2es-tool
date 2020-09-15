package common

import (
	"github.com/elastic/go-elasticsearch/v7"
	"log"
)

func CreateIndex(elasticSearchClient *elasticsearch.Client, indexName string) {
	res, err := elasticSearchClient.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("→ ES →→ Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot create index: %s", res)
	}
}
