package config

import "github.com/spf13/viper"

type OutboxGC struct {
	Interval int
	Timeout  int
	Retries  int
}

func (o *OutboxGC) GetOutboxGC() {
	o.Interval = viper.GetInt("outboxGC.interval")
	o.Timeout = viper.GetInt("outboxGC.timeout")
	o.Retries = viper.GetInt("outboxGC.retries")
}
