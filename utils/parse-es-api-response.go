package utils

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func ParseEsAPIResponse(res *esapi.Response) map[string]interface{} {
	responseBody := make(map[string]interface{})
	json.NewDecoder(res.Body).Decode(&responseBody)
	return responseBody
}

