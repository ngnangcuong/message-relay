package remover

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"message-relay/internal/app/logger"
)

//
type Remover struct {
	logger   logger.Logger
	interval time.Duration
	done     chan struct{}
	db       *sql.DB
}

//
type RemoverParams struct {
	Logger   logger.Logger
	Interval time.Duration
	DB       *sql.DB
}

//
func NewRemover(removerParams RemoverParams) *Remover {
	return &Remover{
		logger:   removerParams.Logger,
		interval: removerParams.Interval,
		db:       removerParams.DB,
		done:     make(chan struct{}),
	}
}

//
func (r *Remover) Start(wg *sync.WaitGroup) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		timer := time.NewTimer(r.interval)
		for {
			select {
			case <-r.done:
				timer.Stop()
				return

			case <-timer.C:
				r.do()
				timer.Reset(r.interval)
			}
		}
	}()
}

func (r *Remover) ShutDown() {
	r.logger.Debug("Remover is shutting down")
	r.done <- struct{}{}
}

func (r *Remover) do() {
	fmt.Println("Do2")
}
