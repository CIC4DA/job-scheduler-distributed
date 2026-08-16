package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/broker"
	"jobscheduler/internal/scheduler"
)

func main() {
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(dbCtx, 
	"postgres://postgres:postgres@localhost:5432/jobscheduler")
	if err != nil {
		log.Fatalf("unable to create connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(dbCtx); err != nil {
		log.Fatalf("unable to reach database: %v", err)
	}
	fmt.Println("scheduler: connected to database")

	producer := broker.NewProducer([]string{"localhost:9092"}, broker.TopicJobs)
	defer producer.Close()
	publisher := broker.NewJobPublisher(producer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dispatcher := scheduler.NewDispatcher(pool, publisher, 1*time.Second, 5)
	dispatcher.Run(ctx)

	fmt.Println("scheduler: shut down cleanly")
}