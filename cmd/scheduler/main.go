package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/broker"
	"jobscheduler/internal/config"
	"jobscheduler/internal/logger"
	"jobscheduler/internal/scheduler"
)

func main() {
	log := logger.New("scheduler")

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

	producer := broker.NewProducer(cfg.KafkaBrokers, broker.TopicJobs)
	defer producer.Close()
	publisher := broker.NewJobPublisher(producer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dispatcher := scheduler.NewDispatcher(pool, publisher, log, 1*time.Second, 5)
	monitor := scheduler.NewMonitor(pool, log, 10*time.Second, 15*time.Second, 30*time.Second)

	// Right now there's exactly one cmd/scheduler process. If it dies, dispatching and the monitor sweep both stop entirely — new jobs pile up QUEUED forever (nobody claims them), and dead workers' stuck jobs never get reclaimed either, since the thing that reclaims them just died too. The API and any live workers are fine, but the whole orchestration layer is gone until someone notices and manually restarts it. That's a real single point of failure.
	// The obvious fix — "just run two scheduler processes" — doesn't work naively. If both cmd/scheduler instances independently ran their own Dispatcher.Run and Monitor.Run, you'd get two processes hitting Postgres with the same FetchPending query every tick (wasteful, though SKIP LOCKED at least prevents literal double-claiming) and two monitor sweeps racing on the same conditions. What you actually want is: run N processes for redundancy, but ensure only one of them is ever actively doing the work at a time — with automatic failover the instant that one dies. That's leader election.
	elector := scheduler.NewLeaderElector(pool)

	campaignTicker := time.NewTicker(3 * time.Second)
	defer campaignTicker.Stop()

	isLeader := false
	var loopCancel context.CancelFunc

	for {
		select {
		case <-ctx.Done():
			if isLeader {
				loopCancel()
				elector.Release()
			}
			log.Info().Msg("scheduler shut down cleanly")
			return
		case <-campaignTicker.C:
			if isLeader {
				continue // already leader, nothing to do this tick
			}
			acquired, err := elector.TryAcquire(ctx)
			if err != nil {
				log.Error().Err(err).Msg("leader election check failed")
				continue
			}
			if acquired {
				log.Info().Msg("elected as leader")
				isLeader = true
				var loopCtx context.Context
				loopCtx, loopCancel = context.WithCancel(ctx)
				go dispatcher.Run(loopCtx)
				go monitor.Run(loopCtx)
			}
		}
	}
}
