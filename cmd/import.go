package cmd

import (
	"github.com/GlobalFishingWatch/bq2es-tool/internal/action"
	"github.com/GlobalFishingWatch/bq2es-tool/types"
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
	importCmd.Flags().StringP( "normalize", "n", "", "The property name to normalize")
	importCmd.Flags().StringP( "normalize-endpoint", "", "", "The final endpoint to normalize")
	importCmd.Flags().StringP( "on-error", "e", "reindex", "Action to do if command fails [reindex|delete|keep]")

	viper.BindPFlag("import-project-id", importCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("import-query", importCmd.Flags().Lookup("query"))
	viper.BindPFlag("import-elastic-search-url", importCmd.Flags().Lookup("elastic-search-url"))
	viper.BindPFlag("import-index-name", importCmd.Flags().Lookup("index-name"))
	viper.BindPFlag("import-import-mode", importCmd.Flags().Lookup("import-mode"))
	viper.BindPFlag("import-normalize", importCmd.Flags().Lookup("normalize"))
	viper.BindPFlag("import-normalize-endpoint", importCmd.Flags().Lookup("normalize-endpoint"))
	viper.BindPFlag("import-on-error", importCmd.Flags().Lookup("on-error"))

	rootCmd.AddCommand(importCmd)
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from BigQuery to Elasticsearch",
	Long:  `Import data from BigQuery to Elasticsearch
Format:
	bq2es-tool import --project-id= --query= --elastic-search-url= --index-name= --normalize=
Example:
	bq2es-tool import 
		--project-id=world-fishing-827 
		--query="SELECT * FROM vessels" 
		--normalize=shipname 
		--normalize-endpoint=https://us-central1-world-fishing-827.cloudfunctions.net/normalize_shipname_http 
		--elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		params := types.ImportParams{
			Query:            viper.GetString("import-query"),
			ElasticSearchUrl: viper.GetString("import-elastic-search-url"),
			ProjectId:        viper.GetString("import-project-id"),
			IndexName:        viper.GetString("import-index-name"),
			ImportMode:       viper.GetString("import-import-mode"),
			Normalize:        viper.GetString("import-normalize"),
			NormalizeEndpoint:viper.GetString("import-normalize-endpoint"),
			OnError:          viper.GetString("import-on-error"),
		}

		log.Println("→ Executing Import command")
		action.ImportBigQueryToElasticSearch(params)
	},
}

