package pkg

import (
	"github.com/GlobalFishingWatch/bq2es-tool/internal/action"
	"github.com/GlobalFishingWatch/bq2es-tool/types"
)

func ImportData(params types.ImportParams) {
	action.ImportBigQueryToElasticSearch(params)
}