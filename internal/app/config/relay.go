package config

import "github.com/spf13/viper"

type Relay struct {
	MaxSize     int8
	Concurrency int
	Timeout     int
}

func (r *Relay) GetRelay() {
	r.MaxSize = int8(viper.GetInt("relay.max_size"))
	r.Concurrency = viper.GetInt("relay.concurrency")
	r.Timeout = viper.GetInt("relay.timeout")
}
