package repository

import (
	"context"
	"jobscheduler/internal/models"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func CreateJob(ctx context.Context, pool *pgxpool.Pool, job *models.Job) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, status) values ($1, $2, $3, $4)`,
		job.Id, job.Type, job.Payload, job.Status.String(),
	)
	return err
}

func GetJob(ctx context.Context, pool *pgxpool.Pool, id string) (*models.Job, error) {
	var job models.Job
	var status string
	err := pool.QueryRow(ctx,
		`SELECT id, type, payload, status, worker_id FROM jobs WHERE id = $1`,
		id,
	).Scan(&job.Id, &job.Type, &job.Payload, &status, &job.WorkerId)

	if err != nil {
		return nil, err
	}

	job.Status = models.ParseJobStatus(status)
	return &job, nil
}

func ListJobs(ctx context.Context, pool *pgxpool.Pool) ([]*models.Job, error) {
	rows, err := pool.Query(ctx,
		`SELECT id, type, payload, status, worker_id FROM jobs ORDER BY created_at DESC`,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var jobs []*models.Job
	for rows.Next() {
		var job models.Job
		var status string
		if err := rows.Scan(&job.Id, &job.Type, &job.Payload, &status, &job.WorkerId); err != nil {
			return nil, err
		}
		job.Status = models.ParseJobStatus(status)
		jobs = append(jobs, &job)
	}

	return jobs, rows.Err()
}

func CancelJob(ctx context.Context, pool *pgxpool.Pool, id string) error {
	_, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'CANCELLED', updated_at = now() WHERE id = $1`,
		id,
	)
	return err
}

func FetchPending(ctx context.Context, pool *pgxpool.Pool, limit int) ([]*models.Job, error) {
	rows, err := pool.Query(ctx, `
	 	WITH CLAIMED AS (
			SELECT Id 
			FROM jobs
			WHERE status = 'QUEUED'
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		) 
		UPDATE jobs
		SET status = 'RUNNING', updated_at = Now()
		WHERE id IN (SELECT Id FROM Claimed)
		RETURNING id, type, payload
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		var job models.Job
		if err := rows.Scan(&job.Id, &job.Type, &job.Payload); err != nil {
			return nil, err
		}
		job.Status = models.Running
		jobs = append(jobs, &job)
	}
	return jobs, rows.Err()
}

func MarkCompleted(ctx context.Context, pool *pgxpool.Pool, jobID string) error {
	_, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'COMPLETED', updated_at = now() WHERE id = $1`,
		jobID,
	)
	return err
}

func RollbackToQueued(ctx context.Context, pool *pgxpool.Pool, jobID string) error {
	_, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'QUEUED', updated_at = now() WHERE id = $1`,
		jobID,
	)
	return err
}

func AssignWorker(ctx context.Context, pool *pgxpool.Pool, jobID string, workerID string) error {
	_, err := pool.Exec(ctx,
		`UPDATE jobs SET worker_id = $1, updated_at = now() WHERE id = $2`,
		workerID, jobID,
	)
	return err
}

func RequeueStale(ctx context.Context, pool *pgxpool.Pool, maxRunTime time.Duration) error {
	_, err := pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'QUEUED', worker_id = NULL, updated_at = now()
		WHERE status = 'RUNNING'
		AND (
			worker_id IN (SELECT id FROM workers WHERE status = 'UNHEALTHY')
			OR updated_at < now() - $1::interval
		)
	`, maxRunTime.String())
	return err
}
