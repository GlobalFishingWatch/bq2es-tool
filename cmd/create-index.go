package cmd

import (
	"bq2es/internal/action"
	"github.com/spf13/cobra"
	"log"
)

func init() {
	createIndexCmd.Flags().StringVarP(&BucketName,"bucket-name", "b", "", "Bucket name used to get mapping file (required)")
	createIndexCmd.MarkFlagRequired("bucket-name")
	createIndexCmd.Flags().StringVarP(&IndName, "index-name", "i", "", "The name of the destination index (required)")
	createIndexCmd.MarkFlagRequired("index-name")
	createIndexCmd.Flags().StringVarP(&ESUrl, "elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	createIndexCmd.MarkFlagRequired("elastic-search-url")
	rootCmd.AddCommand(createIndexCmd)
}

var ProId string
var BucketName string
var IndName string
var ESUrl string

var createIndexCmd = &cobra.Command{
	Use:   "create-index",
	Short: "Create new index applying a custom mapping",
	Long:  `Create new index applying a custom mapping
Format:
	bq2es create-index --bucket-name=[name] --index-name=[name] --elastic-search-url=[url]
Example:
	bq2es create-index --bucket-name=elastic-search-mappings --index-name=test-vessels --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing Create Index command")
		action.CreateIndexWithCustomMapping(BucketName, IndName, ESUrl)
	},
}

