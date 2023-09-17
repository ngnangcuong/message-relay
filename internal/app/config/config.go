package config

import (
	"github.com/spf13/viper"
)

const CONFIG = "./config.yaml"

type Config struct {
	Logger   Logger
	Database Database
}

func Load() (*Config, error) {
	var c Config
	viper.SetConfigFile(CONFIG)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	setDefaultConfig()
	return &c, nil
}

func setDefaultConfig() {

}
