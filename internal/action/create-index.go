package action

import (
	"bq2es/internal/common"
	"bq2es/internal/utils"
	"cloud.google.com/go/storage"
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

func CreateIndexWithCustomMapping(bucketName string, indexName string, elasticSearchUrl string) {
	var (
		res *esapi.Response
		err error
	)
	ctx := context.Background()

	utils.ValidateUrl(elasticSearchUrl)

	storageClient := common.CreateStorageClient(ctx)
	defer storageClient.Close()

	bucket := getBucket(ctx, storageClient, bucketName)
	obj := bucket.Object(indexName + ".json")
	reader, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatalf("→ SG →→ Failed to get reader: %v", err)
	}
	defer reader.Close()

	elasticClient := common.CreateElasticSearchClient(elasticSearchUrl)
	common.CreateIndex(elasticClient, indexName)
	res = putMapping(elasticClient, indexName, reader)
	log.Printf("→ Set Mapping response: %v", res)
}

func getBucket(ctx context.Context, storageClient *storage.Client, bucketName string) *storage.BucketHandle {
	bucket := storageClient.Bucket(bucketName)
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		log.Fatalf("→ SG →→ Failed to get bucket: %v", err)
	}
	log.Printf("→ SG →→ bucket %s, created at %s, is located in %s with storage class %s\n",
		attrs.Name, attrs.Created, attrs.Location, attrs.StorageClass)
	return bucket
}

func putMapping(elasticClient *elasticsearch.Client, indexName string, reader *storage.Reader) *esapi.Response {
	res, err := elasticClient.Indices.PutMapping(reader, func(index *esapi.IndicesPutMappingRequest) {
		index.Index = []string{indexName}
	})
	if err != nil {
		log.Fatalf("→ Error creating Index: %s", err)
	}
	return res
}