package poller

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"message-relay/internal/app/logger"
	"message-relay/internal/app/storage"
)

//
type Poller struct {
	logger            logger.Logger
	done              chan struct{}
	repo              storage.IRepo
	sema              chan struct{}
	largestPreviousID int64
	maxSize           int8
	timeout           time.Duration
}

//
type PollerParams struct {
	Logger      logger.Logger
	DB          *sql.DB
	MaxSize     int8
	Concurrency int
}

//
func NewPoller(pollerParams PollerParams) *Poller {
	return &Poller{
		logger:            pollerParams.Logger,
		done:              make(chan struct{}),
		repo:              storage.NewRepo(pollerParams.DB),
		largestPreviousID: int64(0),
		maxSize:           pollerParams.MaxSize,
		sema:              make(chan struct{}, pollerParams.Concurrency),
	}
}

//
func (p *Poller) Start(wg *sync.WaitGroup) {
	p.logger.Info("Poller is starting")
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-p.done:
				return
			default:
				p.do()
			}
		}
	}()
}

//
func (p *Poller) ShutDown() {
	p.logger.Info("Poller is shutting down")
	p.done <- struct{}{}
}

func (p *Poller) do() {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	events, err := p.repo.Get(ctx, p.largestPreviousID, p.maxSize)
	if err != nil {
		p.logger.Error("")
		return
	}

	if len(events) == 0 {
		time.Sleep(1 * time.Second)
		return
	}

}
