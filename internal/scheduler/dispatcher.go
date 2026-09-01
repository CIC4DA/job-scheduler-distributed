package scheduler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"jobscheduler/internal/models"
	"jobscheduler/internal/repository"
)

// Publisher is an interface, not a concrete type — Dispatcher doesn't need to
// know it's Kafka underneath. Anything with a PublishJob method satisfies this
// (broker.JobPublisher does). This is how Go does dependency injection: no
// framework, just "accept the smallest interface that does what I need."
type Publisher interface {
	PublishJob(ctx context.Context, job *models.Job) error
}

type Dispatcher struct {
	pool 		*pgxpool.Pool
	publisher 	Publisher
	log 		zerolog.Logger
	interval 	time.Duration
	batchSize 	int
}

func NewDispatcher(pool *pgxpool.Pool, publisher Publisher, log zerolog.Logger, interval time.Duration, batchSize int) *Dispatcher {
	return &Dispatcher{pool: pool, publisher: publisher, log: log, interval: interval, batchSize: batchSize}
}

func (d *Dispatcher) Run(ctx context.Context) {
	// Ticker fires once every d.interval (1s, as wired in main), forever,
	// until we stop it. ticker.C is a channel that receives a value each tick.
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// close(jobs)
			return
		// Once per tick: go ask Postgres for a fresh batch of claimable jobs.
		case <-ticker.C:
			pending, err := repository.FetchPending(ctx, d.pool, d.batchSize)
			if err != nil {
				d.log.Error().Err(err).Msg("failed to fetch pending jobs")
				continue
			}
			// for _, job := range pending {
			// 	select {
			// 	case <-ctx.Done():
			// 		close(jobs)
			// 		return
			// 	// The actual handoff to the Executor. This blocks if the Executor's semaphore is currently full
			// 	// full semaphore blocks the Executor's receive loop, which blocks this send
			// 	// which pauses this whole dispatch loop from moving
			// 	// on to the next job or the next tick.
			// 	case jobs <- job:
				// delivery to worker
			// 	}
			// }

			//The dispatcher's job ends the moment it hands the message to Kafka — it no longer owns delivery to a worker.
			for _, job := range pending {
				if err := d.publisher.PublishJob(ctx, job); err != nil {
					d.log.Error().Err(err).Str("job_id", job.Id).Msg("failed to publish job, rolling back")
					if rbErr := repository.RollbackToQueued(ctx, d.pool, job.Id); rbErr != nil {
						d.log.Error().Err(rbErr).Str("job_id", job.Id).Msg("rollback also failed")
					}
				}
			}

		}
	}
}