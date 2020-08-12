package cmd

import (
	"github.com/GlobalFishingWatch/bq2es-tool/internal/action"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func init() {
	createIndexCmd.Flags().StringP("bucket-name", "b", "", "Bucket name used to get mapping file (required)")
	createIndexCmd.MarkFlagRequired("bucket-name")
	createIndexCmd.Flags().StringP( "index-name", "i", "", "The name of the destination index (required)")
	createIndexCmd.MarkFlagRequired("index-name")
	createIndexCmd.Flags().StringP( "elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	createIndexCmd.MarkFlagRequired("elastic-search-url")

	viper.BindPFlag("bucket-name", createIndexCmd.Flags().Lookup("bucket-name"))
	viper.BindPFlag("create-index-name", createIndexCmd.Flags().Lookup("index-name"))
	viper.BindPFlag("create-index-elastic-search-url", createIndexCmd.Flags().Lookup("elastic-search-url"))

	rootCmd.AddCommand(createIndexCmd)
}

var createIndexCmd = &cobra.Command{
	Use:   "create-index",
	Short: "Create new index applying a custom mapping",
	Long:  `Create new index applying a custom mapping
Format:
	bq2es create-index --bucket-name=[name] --index-name=[name] --elastic-search-url=[url]
Example:
	bq2es create-index --bucket-name=elastic-search-mappings --index-name=test-vessels --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		bucketName := viper.GetString("bucket-name")
		indexName := viper.GetString("create-index-name")
		elasticSearchUrl := viper.GetString("create-index-elastic-search-url")
		log.Println("→ Executing Create Index command")
		action.CreateIndexWithCustomMapping(bucketName, indexName, elasticSearchUrl)
	},
}

