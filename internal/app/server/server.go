package server

import (
	"database/sql"
	"log"
	"sync"
	"time"

	"message-relay/internal/app/config"
	"message-relay/internal/app/logger"
	"message-relay/internal/app/outboxGC"
	"message-relay/internal/app/relay"
	"message-relay/internal/app/storage"

	es "github.com/elastic/go-elasticsearch"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

//
type Server struct {
	logger logger.Logger
	wg     sync.WaitGroup

	relay    *relay.Relay
	outboxGC *outboxGC.OutboxGC
}

// NewServer returns a new server with given configuration
func NewServer(config *config.Config, db *sql.DB, es *es.Client, kafka *kafka.Producer) *Server {
	logger, err := logger.GetLogger(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	repo := storage.NewRepo(db, config.Database.Table)

	relay := relay.NewRelay(relay.RelayParams{
		Logger:      logger,
		Repo:        repo,
		DB:          db,
		Es:          es,
		Kafka:       kafka,
		MaxSize:     config.Relay.MaxSize,
		Concurrency: config.Relay.Concurrency,
		TimeOut:     time.Duration(config.Relay.Timeout) * time.Second,
	})

	outboxGC := outboxGC.NewOutboxGC(outboxGC.OutboxGCParams{
		Logger:   logger,
		Interval: time.Duration(config.OutboxGC.Interval) * time.Second,
		TimeOut:  time.Duration(config.OutboxGC.Timeout) * time.Second,
		Retries:  int8(config.OutboxGC.Retries),
		Repo:     repo,
	})

	return &Server{
		logger:   logger,
		relay:    relay,
		outboxGC: outboxGC,
	}
}

//
func (s *Server) Start() error {
	s.logger.Info("Starting Message Relay")
	s.relay.Start(&s.wg)
	s.outboxGC.Start(&s.wg)
	return nil
}

//
func (s *Server) Shutdown() error {
	s.logger.Info("Starting graceful shutdown Message Relay")

	s.relay.ShutDown()
	s.outboxGC.ShutDown()
	s.wg.Wait()

	s.logger.Info("Exitting")
	return nil
}
