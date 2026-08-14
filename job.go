package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"github.com/google/uuid"
	"encoding/json"
	"net/http"
	"github.com/go-chi/chi/v5"
)

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
)

func NewJob(jobType string, payload string) *Job {
	return &Job{
		Id : uuid.New().String(),
		Type : jobType,
		Payload : payload,
		Status : Queued,
	}
}

func GetJob(ctx context.Context, pool *pgxpool.Pool, id string) (*Job, error) {
	var job Job
	var status string
	err := pool.QueryRow(ctx,
		`SELECT id, type, payload, status FROM jobs WHERE id = $1`,
		id,	
	).Scan(&job.Id, &job.Type, &job.Payload, &status)

	if err != nil {
		return nil, err
	}

	job.Status = ParseJobStatus(status)
	return &job, nil
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
	default:
		return Queued
	}
}


// HTTP HANDLERS
func submitJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Type string `json:"type"`
			Payload string `json: "payload"`
		}

		if err := json.NewDecoder (r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		job := NewJob(req.Type, req.Payload)
		if err := CreateJob(r.Context(), pool, job); err != nil {
			http.Error(w, "failed to save job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(job)
	}
}

func getJobHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request){
		id := chi.URLParam(r, "id")
		job, err := GetJob(r.Context(), pool, id)

		if err != nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	}
}

// Methods in Go aren't defined inside the struct like in Java/Python. They're regular functions with an extra "receiver" argument before the name, which attaches the function to a type:
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
	default:
		return "UNKNOWN"
	}
}

// A channel is a typed, thread-safe pipe you send values into and receive values out of. It's Go's core tool for goroutines (lightweight concurrent functions) to talk to each other — the language proverb is "don't communicate by sharing memory, share memory by communicating." This is exactly the pattern the original project uses everywhere (the jobChan in its worker, Kafka message channels, etc.), just with a channel instead of Kafka for now.

// ch := make(chan *Job)       // create a channel that carries *Job values
// ch <- job                   // send job into the channel
// receivedJob := <-ch         // receive a job out of the channel
// close(ch)                   // signal "no more values coming"
// for job := range ch {       // receive values until the channel is closed
// 	// use job
// }

//Goroutines
// go someFunc() runs someFunc concurrently instead of blocking. That's the entire syntax for starting a goroutine — no thread pools, no Promise/async wrapping.

// func worker(jobs <- chan* Job){
// 	for job := range jobs {
// 		job.Status = Running;
// 		fmt.Println("processing: ", job)
// 		job.Status = Completed
// 		fmt.Println("done: ", job)
// 	}
// }

// with sync.WaitGroup
// defer wg.Done() — defer schedules a call to run right before the function returns, no matter which return path or panic causes it to exit. Putting wg.Done() in a defer guarantees it fires even if worker exits abnormally later — a very common Go idiom for cleanup/bookkeeping.
func worker(jobs <- chan* Job, wg *sync.WaitGroup){
	defer wg.Done()
	for job := range jobs {
		job.Status = Running;
		fmt.Println("processing: ", job)
		job.Status = Completed
		fmt.Println("done: ", job)
	}
}

// Go has no built-in semaphore type — you build one from a buffered channel used purely as a set of tokens:
// sem := make(chan struct{}, 3) // capacity 3 = max 3 concurrent
// sem <- struct{}{}             // acquire a slot (blocks if all 3 are taken)
// <-sem                         // release a slot
// struct{}{} is an empty struct value — a type with zero fields, occupying zero bytes.
// We don't care what's in the channel, only whether a slot is free, 
// so this is the idiomatic "I just need a signal, not data" type in Go.

type Executor struct {
	pool    *pgxpool.Pool
	sem 		chan struct {}
	wg 			sync.WaitGroup
	// atomic.Int32 instead of a plain int32. running gets incremented/decremented from many goroutines concurrently. A plain int32 with running++/running-- is a data race — two goroutines can both read the same value, both add 1, and one update gets lost. atomic.Int32's .Add() method uses a CPU-level atomic instruction so concurrent updates never step on each other, without needing a full mutex lock.
	running 	atomic.Int32
}

func NewExecutor(pool *pgxpool.Pool, maxConcurrent int) *Executor {
	return &Executor{
		pool: pool,
		sem:  make(chan struct{}, maxConcurrent),
	}
}


// func (e * Executor) Run(jobs <- chan*Job){
// 	for job:= range jobs{
// 		// The backpressure is free. The line e.sem <- struct{}{} sits inside the for job := range jobs loop, before spawning the goroutine. If 3 jobs are already in flight, this send blocks — which means the loop stops pulling new jobs off jobs until a slot frees up. You get "don't accept more work than you can run" without writing any explicit throttling logic.
// 		e.sem <- struct{}{}
// 		e.wg.Add(1)
// 		e.running.Add(1)

// 		go func(j * Job){
// 			defer func(){
// 				<-e.sem
// 				e.wg.Done()
// 				e.running.Add(-1)
// 			}()

// 			j.Status = Running
// 			fmt.Println("processing:", j)
// 			// Just to test
// 			time.Sleep(500 * time.Millisecond)
// 			j.Status = Completed
// 			fmt.Println("done:", j)
// 		}(job)
// 	}
// 	e.wg.Wait()
// }

//A Context is a value you pass down through function calls that carries a cancellation signal (and, later, deadlines/timeouts and request-scoped data). The key method is Done(), which returns a channel that starts empty and gets closed the moment the context is cancelled — and remember, a receive on a closed channel returns immediately. So <-ctx.Done() is how you ask "has cancellation happened yet?" in a select.
//signal.NotifyContext is a helper that gives you a context tied to an OS signal — e.g. it gets cancelled automatically when the user hits Ctrl+C:

func (e *Executor) Run(ctx context.Context, jobs <-chan *Job) {
//select here means: "whichever of these two channels has something ready first, do that branch." Two new things versus your range jobs version:
//job, ok := <-jobs — receiving from a channel actually returns two values: the value itself, and ok, a bool that's false specifically when the channel is closed and drained. This is the manual version of what range was doing for you automatically.
//On ctx.Done(), we stop pulling new jobs immediately, but still call e.wg.Wait() — letting whatever's already spawned finish before returning. That's the "graceful" part: cancellation stops new work, doesn't kill work in progress.
	for {
		select {
		case <-ctx.Done():
			e.wg.Wait()
			return
		case job, ok := <-jobs:
			if !ok {
				e.wg.Wait()
				return
			}
			e.sem <- struct{}{}
			e.wg.Add(1)
			e.running.Add(1)

			go func(j *Job) {
				defer func() {
					<-e.sem
					e.wg.Done()
					e.running.Add(-1)
				}()
				j.Status = Running
				fmt.Println("processing:", j)
				time.Sleep(2 * time.Second)
				j.Status = Completed
				fmt.Println("done:", j)

				writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := MarkCompleted(writeCtx, e.pool, j.Id); err != nil {
					fmt.Println("failed to mark job completed:", err)
				}
			}(job)
		}
	}
}

// to store job in db
func CreateJob(ctx context.Context, pool *pgxpool.Pool, job *Job) error {
	_ , err := pool.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, status) Values ($1, $2, $3, $4)`,
		job.Id, job.Type, job.Payload, job.Status.String(),
	)
	return err
}

func FetchPending(ctx context.Context, pool *pgxpool.Pool, limit int) ([]* Job, error) {
	// FOR UPDATE tells Postgres "lock every row I'm about to touch, as part of my transaction, so nobody else can also select-and-update them until I'm done." SKIP LOCKED then adds: "if a row is already locked by someone else's in-flight claim, don't wait for them — just skip it and give me a different one instead."
	rows, err := pool.Query(ctx,`
		With Claimed AS (
			Select Id
			from jobs
			where status = 'QUEUED'
			Order by created_at
			FOR UPDATE SKIP LOCKED
			Limit $1		
		)
		Update jobs
		set status = 'RUNNING', updated_at = now()
		where id in (select Id from claimed)
		returning id, type, payload
	`, limit)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var jobs []*Job
	for rows.Next(){
		var job Job
		if err := rows.Scan(&job.Id, &job.Type, &job.Payload); err != nil{
			return nil, err
		}
		job.Status = Running
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



func main() {
	// job := NewJob("echo", "hello world")
	// fmt.Println(*job)

	// // with channels
	// jobs := make(chan *Job)

	// go worker(jobs)

	// jobs <- NewJob("echo", "hello world")
	// jobs <- NewJob("echo", "second job")
	// close(jobs)

	// time.Sleep(1 * time.Second)

	// // with channels and multiple pool workers
	// jobs := make(chan* Job)
	// var wg sync.WaitGroup

	// numWorkers := 3
	// for i := 0; i<numWorkers; i++ {
	// 	wg.Add(1)
	// 	go worker(jobs, &wg)
	// }

	// for i:=0 ; i<5; i++ {
	// 	jobs <- NewJob("echo", fmt.Sprintf("job #%d", i))
	// }
	// close(jobs)

	// wg.Wait()

	//  spawn one goroutine per job, but gate how many can run at once with a semaphore. Different strategy for the same goal (bounded concurrency)

	/* Your current 3-worker pool has a hard structural limit: exactly 3 long-lived goroutines exist, forever. The real project instead has a single loop that spawns a new goroutine per job as it arrives, but uses a semaphore to cap how many of those goroutines may be "in flight" simultaneously. 
	This tends to be easier to reason about for shutdown/draining and for reporting "how many jobs are currently running" as a single counter, rather than tracking N independent worker states.*/
	// jobs := make(chan *Job)
	// executor := NewExecutor(3)

	// go func() {
	// 	for i := 0; i < 5; i++ {
	// 		jobs <- NewJob("echo", fmt.Sprintf("job #%d", i))
	// 	}
	// 	close(jobs)
	// }()

	// executor.Run(jobs)

		
	// postgres connection
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(dbCtx,
	"postgres://postgres:postgres@localhost:5432/jobscheduler")

	if err != nil {
		log.Fatalf("unable to create connection pool: %v", err)
	}
	defer pool.Close()

	if err:= pool.Ping(dbCtx); err != nil {
		log.Fatalf("unable to reach database: %v", err)
	}
	fmt.Println("connected to database")
	
	// Since 5 instant jobs won't let you see shutdown behavior, let's make the producer send jobs continuously (simulating a live stream) and add a delay per job so you have time to hit Ctrl+C mid-run:
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	jobs := make(chan *Job)
	executor := NewExecutor(pool, 3)

	// Note the producer's select too — it needs to be able to abandon a blocked send (jobs <- ...) the instant ctx cancels, otherwise it could hang forever trying to send into a channel nobody's reading from anymore.
	// go func() {
	// 	i := 0
	// 	for {
	// 		select {
	// 			case <-ctx.Done():
	// 				close(jobs)
	// 				return
	// 			default:
	// 			{

	// 				job := NewJob("echo", fmt.Sprintf("job #%d", i))
	// 				if err := CreateJob(ctx, pool, job); err != nil {
	// 					fmt.Println("failed to save job: ", err)
	// 					continue
	// 				}

	// 				select{
	// 				case <- ctx.Done():
	// 					close(jobs)
	// 					return
	// 				case jobs <- job:
	// 					i++
	// 					time.Sleep(300 * time.Millisecond)
	// 				}
	// 			}
	// 		}
	// 	}
	// }()

	// Your producer goroutine should now only write to Postgres — it shouldn't know or care about the jobs channel or the executor at all (this is the real architectural shift: submission and execution are now fully independent, exactly like a real HTTP client submitting a job has no idea which worker will eventually run it):
	// go func() {
	// 	i := 0
	// 	for {
	// 		select {
	// 		case <- ctx.Done():
	// 			return
	// 		default:
	// 			{
	// 				job := NewJob("echo", fmt.Sprintf("job #%d", i))
	// 				if err := CreateJob(ctx, pool ,job); err != nil {
	// 					fmt.Println("failed to save job: ", err)
	// 				}
	// 				i++
	// 				time.Sleep(300 * time.Millisecond)
	// 			}
	// 		}
	// 	}
	// }()

	router := chi.NewRouter()
	router.Post("/jobs", submitJobHandler(pool))
	router.Get("/jobs/{id}", getJobHandler(pool))

	server := &http.Server {Addr : ":8080", Handler: router}
	go func() {
		if err:= server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("Server error: ", err)
		}
	}()

	// Add a separate dispatcher goroutine that owns the jobs channel — polls Postgres on a ticker, feeds whatever it claims into the channel:
	go func() {
		// time.NewTicker(1 * time.Second) gives you a channel (ticker.C) that receives a value once per second, forever — this is the same pattern the reference project's dispatcher uses to poll every 2 seconds.
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <- ctx.Done():
				close(jobs)
				return
			case <- ticker.C:
				pending, err := FetchPending(ctx, pool ,5)
				if err != nil {
					fmt.Println("failed to fetch pending jobs: ", err)
					continue
				}

				for _, job := range pending {
					select {
					case <- ctx.Done():
						close(jobs)
						return
					case jobs <- job:
					}
				}
			}
		}
	}()

	executor.Run(ctx, jobs)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
	fmt.Println("shut down cleanly")

}