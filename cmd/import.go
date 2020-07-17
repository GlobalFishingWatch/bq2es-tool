package cmd

import (
	"bq-to-es-cli/internal/action"
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	importCmd.Flags().StringVarP(&ProjectId,"project-id", "p", "", "Project id related to BigQuery database (required)")
	importCmd.MarkFlagRequired("project-id")
	importCmd.Flags().StringVarP(&Query,"query", "q", "", "Query to find data in BigQuery (required)")
	importCmd.MarkFlagRequired("query")
	importCmd.Flags().StringVarP(&ElasticSearchUrl, "elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	importCmd.MarkFlagRequired("elastic-search-url")
	rootCmd.AddCommand(importCmd)
}

var Query string
var ElasticSearchUrl string
var ProjectId string

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from BigQuery to Elasticsearch",
	Long:  `Import data from BigQuery to Elasticsearch
Format:
	bq-to-es-cli import --query="" --elastic-search-url=""
Example:
	bq-to-es-cli import --project-id=world-fishing-827 --query="SELECT * FROM vessels" --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(">>>>>>> Importing results from big query")
		action.GetResultsFromBigQuery(ProjectId, Query)
	},
}

