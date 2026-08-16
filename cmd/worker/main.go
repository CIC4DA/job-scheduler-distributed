package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/broker"
	"jobscheduler/internal/executor"
	"jobscheduler/internal/models"
)

func main() {
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(dbCtx, "postgres://postgres:postgres@localhost:5432/jobscheduler")
	if err != nil {
		log.Fatalf("unable to create connection pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(dbCtx); err != nil {
		log.Fatalf("unable to reach database: %v", err)
	}
	fmt.Println("worker: connected to database")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	consumer := broker.NewConsumer([]string{"localhost:9092"}, broker.TopicJobs, "workers")
	defer consumer.Close()

	jobs := make(chan *models.Job)
	exec := executor.New(pool, 3)

	// This goroutine is the bridge: Kafka message in, *models.Job out on a
	// plain Go channel. Executor.Run has no idea Kafka exists — it just
	// ranges over a channel like it always did.
	go func() {
		defer close(jobs)
		for {
			msg, err := consumer.FetchMessage(ctx)
			if err != nil {
				fmt.Println("worker: consumer stopped:", err)
				return
			}

			var job models.Job
			if err := json.Unmarshal(msg.Value, &job); err != nil {
				fmt.Println("worker: bad job message:", err)
				consumer.Commit(ctx, msg)
				continue
			}

			select {
			case jobs <- &job:
			case <-ctx.Done():
				return
			}

			if err := consumer.Commit(ctx, msg); err != nil {
				fmt.Println("worker: commit failed:", err)
			}
		}
	}()

	exec.Run(ctx, jobs)
	fmt.Println("worker: shut down cleanly")
}