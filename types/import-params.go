package types

type ImportParams struct {
	Query string
	ElasticSearchUrl string
	ProjectId string
	IndexName string
	ImportMode string
	Normalize string
	NormalizedPropertyName string
	NormalizeEndpoint string
	OnError string
}