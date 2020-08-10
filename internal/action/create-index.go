package action

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
	"bq2es/utils"
)

func CreateIndexWithCustomMapping(bucketName string, indexName string, elasticSearchUrl string) {
	var (
		res *esapi.Response
		err error
	)
	ctx := context.Background()

	utils.ValidateUrl(elasticSearchUrl)

	storageClient := getStorageClient(ctx)
	defer storageClient.Close()

	bucket := getBucket(ctx, storageClient, bucketName)
	obj := bucket.Object(indexName + ".json")
	reader, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatalf("→ SG →→ Failed to get reader: %v", err)
	}
	defer reader.Close()

	elasticClient := getEsClient(elasticSearchUrl)
	createIndex(elasticClient, indexName)
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

func getStorageClient(ctx context.Context) *storage.Client {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("→ SC →→ Failed to create client: %v", err)
	}
	return client
}

func getEsClient(url string) *elasticsearch.Client {
	cfg := elasticsearch.Config{
		Addresses: []string{url},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("→ SG →→ Error creating the client: %s", err)
	}
	return es
}

func createIndex(elasticClient *elasticsearch.Client, indexName string) {
	res, err := elasticClient.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating the client: %s", err)
	}
	log.Printf("Set Mapping response: %v", res)
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