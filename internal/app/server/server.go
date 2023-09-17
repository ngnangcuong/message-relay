package server

import (
	"database/sql"
	"log"
	"sync"
	"time"

	"message-relay/internal/app/config"
	"message-relay/internal/app/logger"
	"message-relay/internal/app/poller"
	"message-relay/internal/app/remover"
)

//
type Server struct {
	logger logger.Logger
	wg     sync.WaitGroup

	poller  *poller.Poller
	remover *remover.Remover
}

// NewServer returns a new server with given configuration
func NewServer(config *config.Config, db *sql.DB) *Server {
	logger, err := logger.GetLogger(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	poller := poller.NewPoller(poller.PollerParams{
		Logger:   logger,
		Interval: 1 * time.Second,
		DB:       db,
	})

	remover := remover.NewRemover(remover.RemoverParams{
		Logger:   logger,
		Interval: 1 * time.Second,
		DB:       db,
	})

	return &Server{
		logger:  logger,
		poller:  poller,
		remover: remover,
	}
}

//
func (s *Server) Start() error {
	s.logger.Info("Starting Message Relay")
	s.poller.Start(&s.wg)
	s.remover.Start(&s.wg)
	return nil
}

//
func (s *Server) Shutdown() error {
	s.logger.Info("Starting graceful shutdown Message Relay")

	s.poller.ShutDown()
	s.remover.ShutDown()
	s.wg.Wait()

	s.logger.Info("Exitting")
	return nil
}
