package config

import "github.com/spf13/viper"

type Logger struct {
	Level string
	Path  string
}

func (l *Logger) GetLogger() *Logger {
	l.Level = viper.GetString("log.level")
	l.Path = viper.GetString("log.path")
	return l
}
