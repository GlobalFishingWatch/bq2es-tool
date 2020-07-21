package cmd

import (
	"bq2es/internal/action"
	"github.com/spf13/cobra"
	"log"
)

func init() {
	importCmd.Flags().StringVarP(&ProjectId,"project-id", "p", "", "Project id related to BigQuery database (required)")
	importCmd.MarkFlagRequired("project-id")
	importCmd.Flags().StringVarP(&Query,"query", "q", "", "Query to find data in BigQuery (required)")
	importCmd.MarkFlagRequired("query")
	importCmd.Flags().StringVarP(&ElasticSearchUrl, "elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	importCmd.MarkFlagRequired("elastic-search-url")
	importCmd.Flags().StringVarP(&IndexName, "index-name", "i", "", "The name of the destination index (required)")
	importCmd.MarkFlagRequired("index-name")
	importCmd.Flags().StringVarP(&ImportMode, "import-mode", "m", "recreate", "Import mode [recreate|append]")
	rootCmd.AddCommand(importCmd)
}

var Query string
var ElasticSearchUrl string
var ProjectId string
var IndexName string
var ImportMode string

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from BigQuery to Elasticsearch",
	Long:  `Import data from BigQuery to Elasticsearch
Format:
	bq-to-es-cli import --project-id= --query= --elastic-search-url= --index-name= 
Example:
	bq-to-es-cli import --project-id=world-fishing-827 --query="SELECT * FROM vessels" --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Getting results from big query")
		results := action.GetResultsFromBigQuery(ProjectId, Query)
		log.Println("→ Importing results to elasticsearch")
		action.ImportBulk(ElasticSearchUrl, results, IndexName, ImportMode)
	},
}

