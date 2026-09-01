package scheduler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"jobscheduler/internal/repository"
)

type Monitor struct {
	pool       *pgxpool.Pool
	log        zerolog.Logger
	interval   time.Duration
	staleAfter time.Duration
	maxRunTime time.Duration
}

func NewMonitor(pool *pgxpool.Pool, log zerolog.Logger, interval, staleAfter time.Duration, maxRunTime time.Duration) *Monitor {
	return &Monitor{pool: pool, log: log, interval: interval, staleAfter: staleAfter, maxRunTime: maxRunTime}
}

func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := repository.MarkWorkersUnhealthy(ctx, m.pool, m.staleAfter); err != nil {
				m.log.Error().Err(err).Msg("failed to mark unhealthy workers")
				continue
			}
			if err := repository.RequeueStale(ctx, m.pool, m.maxRunTime); err != nil {
				m.log.Error().Err(err).Msg("failed to requeue stale jobs")
			}
		}
	}
}
