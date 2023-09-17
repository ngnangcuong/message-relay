package poller

import (
	"database/sql"
	"sync"
	"time"

	"message-relay/internal/app/logger"
)

//
type Poller struct {
	logger   logger.Logger
	interval time.Duration
	done     chan struct{}
	db       *sql.DB
}

//
type PollerParams struct {
	Logger   logger.Logger
	Interval time.Duration
	DB       *sql.DB
}

//
func NewPoller(pollerParams PollerParams) *Poller {
	return &Poller{
		logger:   pollerParams.Logger,
		interval: pollerParams.Interval,
		db:       pollerParams.DB,
		done:     make(chan struct{}),
	}
}

//
func (p *Poller) Start(wg *sync.WaitGroup) {
	p.logger.Info("Poller is starting")
	wg.Add(1)
	go func() {
		defer wg.Done()

		timer := time.NewTimer(p.interval)

		for {
			select {
			case <-p.done:
				timer.Stop()
				return

			case <-timer.C:
				p.do()
				timer.Reset(p.interval)
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

}
