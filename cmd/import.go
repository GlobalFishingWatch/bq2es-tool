package cmd

import (
	"bq2es/internal/action"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func init() {
	importCmd.Flags().StringP("project-id", "p", "", "Project id related to BigQuery database (required)")
	importCmd.MarkFlagRequired("project-id")
	importCmd.Flags().StringP("query", "q", "", "Query to find data in BigQuery (required)")
	importCmd.MarkFlagRequired("query")
	importCmd.Flags().StringP( "elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	importCmd.MarkFlagRequired("elastic-search-url")
	importCmd.Flags().StringP( "index-name", "i", "", "The name of the destination index (required)")
	importCmd.MarkFlagRequired("index-name")
	importCmd.Flags().StringP( "import-mode", "m", "recreate", "Import mode [recreate|append]")
	importCmd.Flags().StringP( "on-error", "e", "delete", "Action to do if command fails [delete|keep]")

	viper.BindPFlag("project-id", importCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("query", importCmd.Flags().Lookup("query"))
	viper.BindPFlag("elastic-search-url", importCmd.Flags().Lookup("elastic-search-url"))
	viper.BindPFlag("index-name", importCmd.Flags().Lookup("index-name"))
	viper.BindPFlag("import-mode", importCmd.Flags().Lookup("import-mode"))
	viper.BindPFlag("on-error", importCmd.Flags().Lookup("on-error"))

	rootCmd.AddCommand(importCmd)
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from BigQuery to Elasticsearch",
	Long:  `Import data from BigQuery to Elasticsearch
Format:
	bq-to-es-cli import --project-id= --query= --elastic-search-url= --index-name= 
Example:
	bq-to-es-cli import --project-id=world-fishing-827 --query="SELECT * FROM vessels" --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		query := viper.GetString("query")
		elasticSearchUrl := viper.GetString("elastic-search-url")
		projectId := viper.GetString("project-id")
		indexName := viper.GetString("index-name")
		importMode := viper.GetString("import-mode")
		onError := viper.GetString("on-error")

		log.Println("→ Executing Import command")
		action.ImportBigQueryToElasticSearch(query, elasticSearchUrl, projectId, indexName, importMode, onError)
	},
}

