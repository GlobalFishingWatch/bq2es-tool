package action

import (
	"github.com/GlobalFishingWatch/bq2es-tool/internal/common"
	"github.com/GlobalFishingWatch/bq2es-tool/types"
	"github.com/GlobalFishingWatch/bq2es-tool/utils"
	"github.com/elastic/go-elasticsearch/v7"
	"log"
)

func AddAlias(params types.AddAliasParams) {
	utils.ValidateUrl(params.ElasticSearchUrl)
	elasticClient := common.CreateElasticSearchClient(params.ElasticSearchUrl)
	createAlias(elasticClient, params.IndexName, params.Alias)
}

func createAlias(elasticClient *elasticsearch.Client, indexName string, alias string) {
	indices := []string{indexName}
	res, err := elasticClient.Indices.PutAlias(indices, alias)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating new alias: %s", err)
	}
	log.Printf("→ ES →→ Create Alias response: %v", res)
}