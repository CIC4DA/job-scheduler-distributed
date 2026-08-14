package models

import "github.com/google/uuid"

type Job struct {
	Id string
	Type string
	Payload string
	Status JobStatus
}

type JobStatus int

const (
	Queued JobStatus = iota
	Running
	Completed
	Failed
	Cancelled
)

func NewJob(jobType string, payload string) *Job {
	return &Job{
		Id: uuid.New().String(),
		Type: jobType,
		Payload: payload,
		Status: Queued,
	}
}

func (s JobStatus) String() string {
	switch s {
	case Queued:
		return "QUEUED"
	case Running:
		return "RUNNING"
	case Completed:
		return "COMPLETED"
	case Failed:
		return "FAILED"
	case Cancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

func ParseJobStatus(s string) JobStatus {
	switch s {
	case "QUEUED":
		return Queued
	case "RUNNING":
		return Running
	case "COMPLETED":
		return Completed
	case "FAILED":
		return Failed
	case "CANCELLED":
		return Cancelled
	default:
		return Queued
	}
}