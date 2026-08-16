package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/api"
	"jobscheduler/internal/config"
	"jobscheduler/internal/logger"
)

func main() {
	log := logger.New("api")

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

	addr := fmt.Sprintf(":%d", cfg.HTTPPort)
	server := &http.Server{Addr: addr, Handler: api.NewRouter(pool)}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("server error")
		}
	}()
	log.Info().Str("addr", addr).Msg("api listening")

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
	log.Info().Msg("api shut down cleanly")
}