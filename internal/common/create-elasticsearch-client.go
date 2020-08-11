package common

import (
	"github.com/elastic/go-elasticsearch/v7"
	"log"
)

func CreateElasticSearchClient(url string) *elasticsearch.Client {
	cfg := elasticsearch.Config{
		Addresses: []string{url},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating the client: %s", err)
	}
	return client
}