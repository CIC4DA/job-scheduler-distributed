package models

import (
	"time"
	"github.com/google/uuid"
)

type Worker struct {
	Id 				string
	Host 			string
	Status 			WorkerStatus
	CPU				*float64
	Memory			*float64
	LastHeartbeat 	*time.Time
	RunningJobs 	int
}

type WorkerStatus int

const (
	WorkerIdle      WorkerStatus = iota // alive, heartbeating, no jobs running right now
	WorkerActive                        // alive, heartbeating, >=1 job running
	WorkerUnhealthy                     // heartbeat has gone stale — presumed dead
	WorkerOffline                       // shut down cleanly, or never registered
)

func NewWorker(host string) *Worker {
	return &Worker{
		Id: uuid.New().String(),
		Host: host,
		Status: WorkerIdle,
	}
}

func (s WorkerStatus) String() string {
	switch s {
	case WorkerIdle:
		return "IDLE"
	case WorkerActive:
		return "ACTIVE"
	case WorkerUnhealthy:
		return "UNHEALTHY"
	case WorkerOffline:
		return "OFFLINE"
	default:
		return "UNKNOWN"
	}
}