package repository

import (
	"context"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
	"jobscheduler/internal/models"
)

func UpsertHeartbeat(ctx context.Context, pool *pgxpool.Pool, w *models.Worker) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO workers (id, host, status, cpu, memory, running_jobs, last_heartbeat)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			cpu = EXCLUDED.cpu,
			memory = EXCLUDED.memory,
			running_jobs = EXCLUDED.running_jobs,
			last_heartbeat = now()
	`, w.Id, w.Host, w.Status.String(), w.CPU, w.Memory, w.RunningJobs)
	return err
}

func MarkWorkersUnhealthy(ctx context.Context, pool *pgxpool.Pool, staleAfter time.Duration) error {
	_, err := pool.Exec(ctx,
		`UPDATE workers SET status = 'UNHEALTHY' WHERE last_heartbeat < now() - $1::interval AND status != 'UNHEALTHY'`,
		staleAfter.String(),
	)
	return err
}

