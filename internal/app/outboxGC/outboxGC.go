package outboxGC

import (
	"context"
	"sync"
	"time"

	"message-relay/internal/app/logger"
	"message-relay/internal/app/storage"
)

// OutboxGC take responsibily of deleting processed messages/events periodically
type OutboxGC struct {
	logger logger.Logger

	// interval is time period between 2 consecutive OutboxGC's action
	interval time.Duration
	// timeout is maximum period when OutboxGC make a query to database
	// After timeout, a query is considered as failed
	timeout time.Duration

	repo storage.IRepo
	// After a failed query to database, OutboxGC will try again until it succeeds or reaches the 'retries' threshole
	retries int8

	// done receive signals when message-relay about to shutdown
	done chan struct{}
}

// OutboxGCParams contains required parameters to declare a new OutboxGC
type OutboxGCParams struct {
	Logger   logger.Logger
	Interval time.Duration
	TimeOut  time.Duration
	Repo     storage.IRepo
	Retries  int8
}

// NewOutboxGC returns a new OutboxGC
func NewOutboxGC(outboxGCParams OutboxGCParams) *OutboxGC {
	return &OutboxGC{
		logger:   outboxGCParams.Logger,
		interval: outboxGCParams.Interval,
		timeout:  outboxGCParams.TimeOut,
		repo:     outboxGCParams.Repo,
		retries:  outboxGCParams.Retries,
		done:     make(chan struct{}),
	}
}

//
func (o *OutboxGC) Start(wg *sync.WaitGroup) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		timer := time.NewTimer(o.interval)
		for {
			select {
			case <-o.done:
				timer.Stop()
				return

			case <-timer.C:
				o.do()
				timer.Reset(o.interval)
			}
		}
	}()
}

//
func (r *OutboxGC) ShutDown() {
	r.logger.Debug("OutboxGC is shutting down")
	r.done <- struct{}{}
}

//
func (o *OutboxGC) do() {
	if err := o.repo.Delete(context.Background(), o.timeout); err != nil {
		// if errors.Is(err, context.DeadlineExceeded) {
		// 	r.retry()
		// }
		o.logger.Error("")
		o.retry()
		return
	}
	o.logger.Infof("")
}

func (o *OutboxGC) retry() {
	o.logger.Debug()
	for i := 1; i <= int(o.retries); i++ {
		o.logger.Debugf("")
		if err := o.repo.Delete(context.Background(), o.timeout); err != nil {
			continue
		}
		o.logger.Debugf("")
		return
	}
	o.logger.Debugf("")
	return
}
