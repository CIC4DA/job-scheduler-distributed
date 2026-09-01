package scheduler

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

//Your pgxpool.Pool is a pool of many database connections, not one.
// If you naively did pool.QueryRow(ctx, "SELECT pg_try_advisory_lock(...)"), pgx would grab some connection from the pool, run the lock call on it, and return that connection to the pool — meaning the lock is now held by some specific pooled connection,
// but you no longer have an exclusive handle to which one.
// Your next query (even the unlock call) might run on a completely different connection that never held the lock — so the unlock silently does nothing, and your lock stays leaked until that original connection happens to get reused or reset.
// This is a genuinely common real-world mistake with Postgres advisory locks + connection pools.

// Arbitrary, fixed number this application always uses to mean "the scheduler leadership lock" - doesn't correspont to any row/table
const leaderLockID = 851001

type LeaderElector struct {
	pool *pgxpool.Pool
	conn *pgxpool.Conn // the ONE dedicated connection holding our lock, once acquired
}

func NewLeaderElector(pool *pgxpool.Pool) *LeaderElector {
	return &LeaderElector{pool: pool}
}

// TryAcquire checks out a dedicated connection and attempts the lock on it,
// without blocking. Return true only if THIS CALL won the lock.
func (le *LeaderElector) TryAcquire(ctx context.Context) (bool, error) {
	conn, err := le.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}

	// Why a Postgres advisory lock specifically
	//Postgres has a built-in primitive for exactly this: pg_try_advisory_lock(<id>) — an application-defined lock keyed to an arbitrary number you choose, with no relationship to any table or row. Multiple database sessions can attempt to grab the same lock ID; exactly one succeeds, everyone else gets false immediately (no blocking/waiting, since we're using the "try" variant rather than the blocking one).
	//The property that makes this genuinely elegant for leader election: an advisory lock is tied to the specific database connection that acquired it, and is automatically released the instant that connection closes — whether by a clean unlock call, a crash, a network drop, anything. That means Postgres itself gives us crash-based failover for free. Contrast this with the worker heartbeat mechanism we just built by hand (heartbeat rows, staleness thresholds, a monitor sweep) — that entire apparatus exists because Kafka/Postgres don't automatically know when a worker dies. For the scheduler's own leadership, we don't need to build any of that ourselves — the lock's lifetime is already tied to the leader process being alive, automatically.
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockID).Scan(&acquired); err != nil {
		conn.Release()
		return false, err
	}

	if !acquired {
		conn.Release() // someone else is leader, we don't need this connection
		return false, nil
	}

	le.conn = conn
	return true, nil
}

func (le *LeaderElector) Release() {
	if le.conn == nil {
		return
	}

	le.conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, leaderLockID)
	le.conn.Release()
	le.conn = nil
}
