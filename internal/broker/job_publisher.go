package broker

import (
	"context"
	"jobscheduler/internal/models"
)

// this is the piece that connects broker to models.Job, keeping producer.go generic
type JobPublisher struct {
	producer *Producer
}

func NewJobPublisher(p *Producer) *JobPublisher{
	return &JobPublisher{producer: p}
}

func (jp *JobPublisher) PublishJob(ctx context.Context, job *models.Job) error {
	return jp.producer.Publish(ctx, job.Id, job)
}

