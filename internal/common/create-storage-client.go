package common

import (
	"cloud.google.com/go/storage"
	"context"
	"log"
)

func CreateStorageClient(ctx context.Context) *storage.Client {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("→ SC →→ Failed to create client: %v", err)
	}
	return client
}