package app

import (
	"os"
	"os/signal"
	"syscall"

	"message-relay/internal/app/config"
	"message-relay/internal/app/server"
	"message-relay/internal/app/storage"
)

var srv *server.Server

func Run() error {
	if err := start(); err != nil {
		return err
	}
	return shutdown()
}

// loadConfig loads all config in file configuration and set some defaults value for some config
func loadConfig() (*config.Config, error) {
	c, err := config.Load()
	if err != nil {
		return nil, err
	}

	c.Logger.GetLogger()
	c.Database.GetDatabase()
	c.OutboxGC.GetOutboxGC()
	c.Relay.GetRelay()
	c.Elastic.GetElastic()
	c.Kafka.GetKafka()

	return c, nil
}

// start() take responsibility of loading configuration and initializing all process/module needed
func start() error {
	c, err := loadConfig()
	if err != nil {
		return err
	}

	db, dErr := storage.GetPostgres(c)
	if dErr != nil {
		return dErr
	}
	es, eErr := storage.GetESClient(c)
	if eErr != nil {
		return eErr
	}
	kafka, kErr := storage.GetKafkaProducer(c)
	if kErr != nil {
		return kErr
	}

	srv = server.NewServer(c, db, es, kafka)
	return srv.Start()
}

// stop() to stop all process/module in order to ensure graceful shutdown
func stop() error {
	return srv.Shutdown()
}

// waitForSignal() to receive TERM signal from client
func waitForSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	for {
		_ = <-sigs
		break
	}
}

// shutdown() is function calling waitForSignal() and stop()
func shutdown() error {
	waitForSignal()
	if err := stop(); err != nil {
		return err
	}

	return nil
}
