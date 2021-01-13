package txmpg

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/williammoran/txmanager"
)

// MakeFinalizer is a constructor for a Postgres
// transaction driver
func MakeFinalizer(
	ctx context.Context, name string, cPool *pgxpool.Pool,
) *Finalizer {
	tx, err := cPool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	var id int64
	err = tx.QueryRow(ctx, "SELECT txid_current()").Scan(&id)
	if err != nil {
		panic(err)
	}
	var pid int64
	err = tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	if err != nil {
		panic(err)
	}
	finalizer := Finalizer{
		ctx:          ctx,
		name:         name,
		pool:         cPool,
		TX:           tx,
		serverTXID:   id,
		serverConnID: pid,
	}
	return &finalizer
}

// Finalizer manages transactions on a PostgreSQL
// server
type Finalizer struct {
	ctx             context.Context
	TraceFlag       bool
	name            string
	pool            *pgxpool.Pool
	TX              pgx.Tx
	serverTXID      int64
	serverConnID    int64
	id              string
	deferredCommits []func() error
	aborted         bool
}

// PgTx returns the underlying SQL transaction object
func (m *Finalizer) PgTx() pgx.Tx {
	return m.TX
}

// Defer registers a function to execute at Finalize time
func (m *Finalizer) Defer(exec func() error) {
	m.Trace("Defer()")
	m.deferredCommits = append(m.deferredCommits, exec)
}

// Finalize executes any deferred commits
func (m *Finalizer) Finalize() error {
	for _, commit := range m.deferredCommits {
		err := commit()
		if err != nil {
			return m.finalizerError(
				txmanager.WrapError(
					err, "Running deferred commits",
				))
		}
	}
	return nil
}

// Commit finishes the transaction
func (m *Finalizer) Commit() error {
	var status string
	err := m.TX.QueryRow(m.ctx, "SELECT txid_status($1)", m.serverTXID).Scan(&status)
	if err != nil {
		return txmanager.WrapError(err, "Commit() failed to get txid_status()")
	}
	m.Trace("transaction status at Commit() '%s'", status)
	if status != "in progress" {
		return fmt.Errorf("Commit on TX in status '%s'", status)
	}
	err = m.TX.Commit(m.ctx)
	if err != nil {
		return txmanager.WrapError(err, "Failed to commit")
	}
	m.Trace("Transaction committed")
	return nil
}

// Abort rolls back the transaction
func (m *Finalizer) Abort() {
	if m.aborted {
		m.Trace("Abort() called, but already aborted")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := m.TX.Rollback(ctx)
	if err != nil {
		m.Trace("During TX.Rollback(): %+v", err)
	}
	m.aborted = true
}

func (m *Finalizer) finalizerError(err error) *txmanager.Error {
	return txmanager.WrapError(
		err,
		fmt.Sprintf(
			"TX: %s PGTXID: %d PGPID: %d message: %s",
			m.id, m.serverTXID, m.serverConnID, err.Error(),
		),
	)
}

const printformat = "%s: TX: %s PGTXID: %d PGPID: %d message: %s"

func (m *Finalizer) panicf(msg string, err error, args ...interface{}) {
	_, f, l, _ := runtime.Caller(1)
	m.printf("PANIC", fmt.Sprintf("called from %s:%d", f, l))
	m.Trace("%T: %+v", err, err)
	ctxErr := m.ctx.Err()
	if ctxErr != nil {
		m.printf("PANIC", fmt.Sprintf("Context error: %s", ctxErr.Error()))
	}
	message := fmt.Sprintf(msg, args...)
	if err != nil {
		message = fmt.Sprintf("%s Error: %s", message, err.Error())
	}
	log.Panicf(
		printformat,
		"PANIC", m.id, m.serverTXID, m.serverConnID, message,
	)
}

// Trace logs a message with details about the IDs
// associated with the finalizer
func (m *Finalizer) Trace(format string, args ...interface{}) {
	if !m.TraceFlag {
		return
	}
	message := fmt.Sprintf(format, args...)
	m.printf("trace", message)
}

// printf prints to the default logger with a prefix
// that includes helpful data for tracing
func (m *Finalizer) printf(prefix, message string) {
	log.Printf(
		printformat,
		prefix, m.id, m.serverTXID, m.serverConnID, message,
	)
}
