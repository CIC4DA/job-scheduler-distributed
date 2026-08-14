package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/models"
	"jobscheduler/internal/repository"
)

type Dispatcher struct {
	pool 		*pgxpool.Pool
	interval 	time.Duration
	batchSize 	int
}

func NewDispatcher(pool *pgxpool.Pool, interval time.Duration, batchSize int) *Dispatcher {
	return &Dispatcher{pool: pool, interval: interval, batchSize: batchSize}
}

func (d *Dispatcher) Run(ctx context.Context, jobs chan<- *models.Job) {
	// Ticker fires once every d.interval (1s, as wired in main), forever,
	// until we stop it. ticker.C is a channel that receives a value each tick.
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			return
		// Once per tick: go ask Postgres for a fresh batch of claimable jobs.
		case <-ticker.C:
			pending, err := repository.FetchPending(ctx, d.pool, d.batchSize)
			if err != nil {
				fmt.Println("failed to fetch pending jobs:", err)
				continue
			}
			for _, job := range pending {
				select {
				case <-ctx.Done():
					close(jobs)
					return
				// The actual handoff to the Executor. This blocks if the Executor's semaphore is currently full
				// full semaphore blocks the Executor's receive loop, which blocks this send
				// which pauses this whole dispatch loop from moving
				// on to the next job or the next tick.
				case jobs <- job:
				}
			}
		}
	}
}