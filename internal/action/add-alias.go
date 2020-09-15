package action

import (
	"github.com/GlobalFishingWatch/bq2es-tool/internal/common"
	"github.com/GlobalFishingWatch/bq2es-tool/utils"
	"github.com/elastic/go-elasticsearch/v7"
	"log"
)

func AddAlias(indexName string, alias string, elasticSearchUrl string) {
	utils.ValidateUrl(elasticSearchUrl)
	elasticClient := common.CreateElasticSearchClient(elasticSearchUrl)
	createAlias(elasticClient, indexName, alias)
}

func createAlias(elasticClient *elasticsearch.Client, indexName string, alias string) {
	indices := []string{indexName}
	res, err := elasticClient.Indices.PutAlias(indices, alias)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating new alias: %s", err)
	}
	log.Printf("→ ES →→ Create Alias response: %v", res)
}