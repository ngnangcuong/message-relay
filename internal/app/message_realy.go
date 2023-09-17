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

func loadConfig() (*config.Config, error) {
	c, err := config.Load()
	if err != nil {
		return nil, err
	}

	c.Logger.GetLogger()

	return c, nil
}

// start() take responsibility of loading configuration and initializing all process/module needed
func start() error {
	c, err := loadConfig()
	if err != nil {
		return err
	}

	db, err := storage.GetPostgres(c)
	if err != nil {
		return err
	}

	srv = server.NewServer(c, db)
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
