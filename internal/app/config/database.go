package config

import "github.com/spf13/viper"

type Database struct {
	Url string
}

func (d *Database) GetDatabase() {
	d.Url = viper.GetString("database.url")
}
