package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"jobscheduler/internal/models"
	"jobscheduler/internal/repository"
)

func NewRouter(pool *pgxpool.Pool) http.Handler {
	router := chi.NewRouter()
	router.Post("/jobs", submitJobHandler(pool))
	router.Get("/jobs/{id}", getJobHandler(pool))
	router.Get("/jobs", listJobHandler(pool))
	router.Delete("/jobs/{id}", cancelJobHandler(pool))
	return router
}

func submitJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Type    string `json:"type"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		job := models.NewJob(req.Type, req.Payload)
		if err := repository.CreateJob(r.Context(), pool, job); err != nil {
			http.Error(w, "failed to save job", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(job)
	}
}

func getJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		job, err := repository.GetJob(r.Context(), pool, id)
		if err != nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	}
}

func listJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobs, err := repository.ListJobs(r.Context(), pool)
		if err != nil {
			http.Error(w, "jobs not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jobs)
	}
}

func cancelJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if err := repository.CancelJob(r.Context(), pool, id); err != nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}