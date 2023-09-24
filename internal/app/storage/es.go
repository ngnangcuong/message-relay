package storage

import (
	"message-relay/internal/app/config"

	es "github.com/elastic/go-elasticsearch"
)

var esClient *es.Client

func GetESClient(config *config.Config) (*es.Client, error) {
	cfg := es.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	}
	es, err := es.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return es, nil
}
