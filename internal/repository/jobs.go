package repository

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"jobscheduler/internal/models"
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
		`SELECT id, type, payload, status FROM jobs WHERE id = $1`,
		id,
	).Scan(&job.Id, &job.Type, &job.Payload, &status)

	if err != nil {
		return nil, err
	}

	job.Status = models.ParseJobStatus(status)
	return &job, nil
}

func ListJobs (ctx context.Context, pool *pgxpool.Pool) ([]*models.Job, error){
	rows, err := pool.Query(ctx, 
		`SELECT id, type, payload, status FROM jobs ORDER BY created_at DESC`,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var jobs []*models.Job
	for rows.Next() {
		var job models.Job
		var status string
		if err := rows.Scan(&job.Id, &job.Type, &job.Payload, &status); err != nil {
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