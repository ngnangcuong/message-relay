package relay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"message-relay/internal/app/logger"
	"message-relay/internal/app/models"
	"message-relay/internal/app/storage"

	es "github.com/elastic/go-elasticsearch"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Relay take responsibility of fetching new messages/events and handling each messages in separated goroutines
type Relay struct {
	logger logger.Logger
	// timeout is maximum period when Relay make a query to database
	// After timeout, a query is considered as failed
	timeout time.Duration

	db    *sql.DB
	repo  storage.IRepo
	es    *es.Client
	kafka *kafka.Producer

	maxSize int8
	// sema is a counting semaphore to ensure number of active gorountines does not exceed the limit
	sema chan struct{}
	// done receive signals when message-relay about to shutdown
	done chan struct{}
	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	quit chan struct{}
}

// RelayParams contains required parameters to declare a new Relay
type RelayParams struct {
	Logger      logger.Logger
	Repo        storage.IRepo
	DB          *sql.DB
	Es          *es.Client
	Kafka       *kafka.Producer
	MaxSize     int8
	Concurrency int
	TimeOut     time.Duration
}

//
func NewRelay(relayParams RelayParams) *Relay {
	return &Relay{
		logger:  relayParams.Logger,
		done:    make(chan struct{}),
		repo:    relayParams.Repo,
		db:      relayParams.DB,
		es:      relayParams.Es,
		kafka:   relayParams.Kafka,
		maxSize: relayParams.MaxSize,
		sema:    make(chan struct{}, relayParams.Concurrency),
		timeout: relayParams.TimeOut,
	}
}

//
func (r *Relay) execTx(ctx context.Context, fn func(storage.IRepo) error) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	repoWithTx := r.repo.WithTx(tx)
	if err := fn(repoWithTx); err != nil {
		if rErr := tx.Rollback(); rErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rErr)
		}
		return err
	}

	return tx.Commit()
}

//
func (r *Relay) Start(wg *sync.WaitGroup) {
	r.logger.Info("Relay is starting")
	wg.Add(1)

	go func() {
		for e := range r.kafka.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					r.logger.Errorf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					r.logger.Infof("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	go func() {
		defer wg.Done()

		for {
			select {
			case <-r.done:
				r.kafka.Close()
				return
			default:
				r.do()
			}
		}
	}()
}

//
func (r *Relay) ShutDown() {
	r.logger.Info("Relay is shutting down")
	r.done <- struct{}{}
}

func (r *Relay) do() {
	// To try to get new and unproccessed messages in database
	events, err := r.repo.Get(context.Background(), r.timeout, r.maxSize)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			time.Sleep(1 * time.Second)
			return
		}
		// TODO: Content
		r.logger.Error("")
		return
	}

	for _, event := range events {
		go r.handle(event)
	}
}

func (r *Relay) handle(event models.Event) {
	select {
	case <-r.quit:
		return
	case r.sema <- struct{}{}:
		err := r.execTx(context.Background(), func(repo storage.IRepo) error {
			_, err := repo.GetAEventForUpdate(context.Background(), r.timeout, event.ID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("")
				}
				r.retry()
			}
			document, kafkaEvent, dErr := r.dataTransform(event)
			// TODO: Error Handling
			if dErr != nil {
				return dErr
			}
			// TODO: Error Handling
			if err := r.sendEventToES(document); err != nil {
				return err
			}
			// TODO: Error Handling
			if err := r.sendEventToKafka(kafkaEvent); err != nil {
				return err
			}
			// TODO: Error Handling
			return repo.UpdateProcessed(context.Background(), r.timeout, event.ID)
			// return nil
		})

		// TODO: Error Handling
		if err != nil {

		}
		// TODO: Content
		r.logger.Info("")
		<-r.sema
		return
	}
}

func (r *Relay) dataTransform(event models.Event) (models.Document, models.KafkaEvent, error) {
	return models.Document{}, models.KafkaEvent{}, nil
}

func (r *Relay) sendEventToES(document models.Document) error {
	return nil
}

func (r *Relay) sendEventToKafka(kafkaEvent models.KafkaEvent) error {
	topic := "myTopic"
	deliverCh := make(chan kafka.Event)

	// TODO: Error Handling
	payload, pErr := json.Marshal(kafkaEvent)
	if pErr != nil {
		return pErr
	}
	// TODO: Delivery chanel
	err := r.kafka.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic}, //, Partition: kafka.PartitionAny},
		Value:          payload,
	}, deliverCh)

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrQueueFull {
			// Producer queue is full, wait 1s for messages
			// to be delivered then try again.
			time.Sleep(time.Second)
		}
		// TODO: Content
		r.logger.Errorf("Failed to produce message: %v\n", err)
	}

	for e := range deliverCh {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				r.logger.Errorf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				r.logger.Infof("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}

	return nil
}

func (r *Relay) retry() {

}
