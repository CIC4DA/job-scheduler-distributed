package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/broker"
	"jobscheduler/internal/scheduler"
	"jobscheduler/internal/config"
	"jobscheduler/internal/logger"
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
	dispatcher.Run(ctx)

	log.Info().Msg("scheduler shut down cleanly")
}