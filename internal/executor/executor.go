package executor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"jobscheduler/internal/models"
	"jobscheduler/internal/repository"
)

type Executor struct {
	pool 	*pgxpool.Pool
	workerID string
	sem 	chan struct {}
	wg 		sync.WaitGroup
	running atomic.Int32
	log		zerolog.Logger
}

func New(pool *pgxpool.Pool, workerID string, maxConcurrent int, log zerolog.Logger) *Executor {
	return &Executor{
		pool: pool,
		workerID: workerID,
		sem: make(chan struct {}, maxConcurrent),
		log: log,
	}
}

func (e *Executor) Run(ctx context.Context, jobs <- chan *models.Job) {
	for {
		select {
		// If shutdown was called, stop pullings jobs. But finish already-spawned goroutienes first
		case <-ctx.Done():
			e.wg.Wait()
			return

		// ok is false specifically when the channel has been closed AND fully drained
		case job, ok := <-jobs:
			if !ok {
				// same gracefull shutdown
				e.wg.Wait()
				return
			}
			// Acquire one semaphore "slot" before spawning a worker.
			e.sem <- struct{}{}
			e.wg.Add(1)
			e.running.Add(1)

			// Spawn a brand-new goroutine to actually run this one job.
			go func(j *models.Job) {
				defer func(){
					<-e.sem
					e.wg.Done()
					e.running.Add(-1)
				}()

				j.Status = models.Running
				e.log.Info().Str("job_id", j.Id).Str("type", j.Type).Msg("processing job")
				if err := repository.AssignWorker(context.Background(), e.pool, j.Id, e.workerID); err != nil {
					e.log.Error().Err(err).Str("job_id", j.Id).Msg("Failed to record worker assignment")
				}

				time.Sleep(2 * time.Second)  // stand-in for real work
				j.Status = models.Completed
				e.log.Info().Str("job_id", j.Id).Msg("job completed")
				
				// Write completion back to Postgres using a FRESH context,
				// not the outer ctx — during shutdown, ctx is already
				// cancelled (that's why we're draining), but we still want
				// this final DB write to succeed.
				writeCtx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
				defer cancel()
				if err := repository.MarkCompleted(writeCtx, e.pool, j.Id); err != nil {
					e.log.Error().Err(err).Str("job_id", j.Id).Msg("failed to mark job completed")
				}

			}(job)
		}
	}
}

func (e *Executor) RunningJobs() int32 {
	return e.running.Load()
}