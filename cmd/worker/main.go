package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/broker"
	"jobscheduler/internal/executor"
	"jobscheduler/internal/models"
	"jobscheduler/internal/config"
	"jobscheduler/internal/logger"
	"jobscheduler/internal/repository"
)

func main() {
	log := logger.New("worker")

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("config load failed")
	}

	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(dbCtx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create connection pool")
	}
	defer pool.Close()

	if err := pool.Ping(dbCtx); err != nil {
		log.Fatal().Err(err).Msg("unable to reach database")
	}
	log.Info().Msg("connected to database")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	consumer := broker.NewConsumer(cfg.KafkaBrokers, broker.TopicJobs, "workers")
	defer consumer.Close()

	// a heartbeat goroutine, reporting every 5 seconds:
	hostname, _ := os.Hostname()
	worker := models.NewWorker(hostname)
	
	jobs := make(chan *models.Job)
	exec := executor.New(pool, worker.Id, 3, log)


	go func(){
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <- ctx.Done():
				return
			case <- ticker.C:
				worker.RunningJobs = int(exec.RunningJobs())
				if exec.RunningJobs() > 0 {
					worker.Status = models.WorkerActive
				} else {
					worker.Status = models.WorkerIdle
				}
				if err := repository.UpsertHeartbeat(ctx, pool, worker); err != nil {
					log.Error().Err(err).Msg("heartbeat failed")
				}
			}
		}
	}()


	// This goroutine is the bridge: Kafka message in, *models.Job out on a
	// plain Go channel. Executor.Run has no idea Kafka exists — it just
	// ranges over a channel like it always did.
	go func() {
		defer close(jobs)
		for {
			msg, err := consumer.FetchMessage(ctx)
			if err != nil {
				log.Error().Err(err).Msg("consumer stopped")
				return
			}

			var job models.Job
			if err := json.Unmarshal(msg.Value, &job); err != nil {
				log.Error().Err(err).Msg("bad job message")
				consumer.Commit(ctx, msg)
				continue
			}

			select {
			case jobs <- &job:
			case <-ctx.Done():
				return
			}

			if err := consumer.Commit(ctx, msg); err != nil {
				log.Error().Err(err).Str("job_id", job.Id).Msg("commit failed")
			}
		}
	}()

	exec.Run(ctx, jobs)
	log.Info().Msg("worker shut down cleanly")
}