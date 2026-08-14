package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/api"
	"jobscheduler/internal/executor"
	"jobscheduler/internal/models"
	"jobscheduler/internal/scheduler"
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
	fmt.Println("connected to database")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	jobs := make(chan *models.Job)
	exec := executor.New(pool, 3)
	dispatcher := scheduler.NewDispatcher(pool, 1*time.Second, 5)

	server := &http.Server{Addr: ":8080", Handler: api.NewRouter(pool)}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("server error:", err)
		}
	}()

	go dispatcher.Run(ctx, jobs)

	exec.Run(ctx, jobs)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
	fmt.Println("shut down cleanly")
}