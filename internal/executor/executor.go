package executor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
	"jobscheduler/internal/models"
	"jobscheduler/internal/repository"
)

type Executor struct {
	pool 	*pgxpool.Pool
	sem 	chan struct {}
	wg 		sync.WaitGroup
	running atomic.Int32
}

func New(pool *pgxpool.Pool, maxConcurrent int) *Executor {
	return &Executor{
		pool: pool,
		sem: make(chan struct {}, maxConcurrent),
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
				fmt.Println("processing: ", j)
				time.Sleep(2 * time.Second)  // stand-in for real work
				j.Status = models.Completed
				fmt.Println("done: ", j)
				
				// Write completion back to Postgres using a FRESH context,
				// not the outer ctx — during shutdown, ctx is already
				// cancelled (that's why we're draining), but we still want
				// this final DB write to succeed.
				writeCtx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
				defer cancel()
				if err := repository.MarkCompleted(writeCtx, e.pool, j.Id); err != nil {
					fmt.Println("failed to mark job completed:", err)
				}

			}(job)
		}
	}
}