package cmd

import (
	"bq2es/internal/action"
	"github.com/spf13/cobra"
	"log"
)

func init() {
	importCmd.Flags().StringVarP(&ProjectId,"project-id", "p", "", "Project id related to Cloud Storage bucket (required)")
	importCmd.MarkFlagRequired("project-id")
	importCmd.Flags().StringVarP(&BucketName,"bucket-name", "b", "", "Bucket name used to get mapping file (required)")
	importCmd.MarkFlagRequired("bucket-name")
	importCmd.Flags().StringVarP(&IndexName, "index-name", "i", "", "The name of the destination index (required)")
	importCmd.MarkFlagRequired("index-name")
	importCmd.Flags().StringVarP(&ElasticSearchUrl, "elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	importCmd.MarkFlagRequired("elastic-search-url")
	rootCmd.AddCommand(importCmd)
}

var ElasticSearchUrl string
var ProjectId string
var IndexName string
var BucketName string

var createIndexCmd = &cobra.Command{
	Use:   "create-index",
	Short: "Create new index applying a custom mapping",
	Long:  `Create new index applying a custom mapping
Format:
	bq2es create-index --project-id=[id] --bucket-name=[name] --index-name=[name] --elastic-search-url=[url]
Example:
	bq2es create-index --project-id=world-fishing-827 --bucket-name=elastic-search-mappings --index-name=test-vessels --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing Create Index command")
	},
}

