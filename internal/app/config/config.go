package config

import (
	"github.com/spf13/viper"
)

const CONFIGPATH = "./config.yaml"

type Config struct {
	Logger   Logger
	Database Database
	OutboxGC OutboxGC
	Relay    Relay
	Elastic  Elastic
	Kafka    Kafka
}

func Load() (*Config, error) {
	var c Config
	viper.SetConfigFile(CONFIGPATH)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	setDefaultConfig()
	return &c, nil
}

func setDefaultConfig() {

}
