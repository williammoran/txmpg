package txmpg

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"

	"github.com/lib/pq"
	"github.com/williammoran/txmanager"
)

// MakeFinalizer is a constructor for a Postgres
// transaction driver
func MakeFinalizer(
	ctx context.Context, name string, cPool *sql.DB,
) *Finalizer {
	tx, err := cPool.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	var id int64
	err = tx.QueryRowContext(ctx, "SELECT txid_current()").Scan(&id)
	if err != nil {
		panic(err)
	}
	var pid int64
	err = tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	if err != nil {
		panic(err)
	}
	finalizer := Finalizer{
		ctx:          ctx,
		pool:         cPool,
		name:         name,
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
	pool            *sql.DB
	TX              *sql.Tx
	serverTXID      int64
	serverConnID    int64
	id              string
	deferredCommits []func() error
}

// PgTx returns the underlying SQL transaction object
func (m *Finalizer) PgTx() *sql.Tx {
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
func (m *Finalizer) Commit() {
	var status string
	err := m.TX.QueryRow("SELECT txid_status($1)", m.serverTXID).Scan(&status)
	if err != nil {
		m.panicf("Commit() failed to get txid_status()", err)
	}
	m.Trace("transaction status at Commit() '%s'", status)
	if status != "in progress" {
		m.panicf("Commit on uncomittable TX", nil)
	}
	err = m.TX.Commit()
	if err != nil {
		m.panicf("Failed to commit", err)
	}
	m.Trace("Transaction committed")
}

// Abort rolls back the transaction
func (m *Finalizer) Abort() {
	var status string
	err := m.TX.QueryRow("SELECT txid_status($1)", m.serverTXID).Scan(&status)
	m.Trace("transaction status at Abort() '%s'", status)
	if status == "in progress" {
		err = m.TX.Rollback()
		if err != nil {
			ctxErr := m.ctx.Err()
			if ctxErr == context.DeadlineExceeded || ctxErr == context.Canceled {
				// If the context was cancelled for any
				// reason, the transaction is already
				// rolled back by the driver
				return
			}
			m.panicf("Failed to roll back", err)
		}
	}
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

func (m *Finalizer) panicf(msg string, err error, args ...interface{}) {
	_, f, l, _ := runtime.Caller(1)
	log.Printf("panicf called from %s:%d", f, l)
	pqerr, ok := err.(*pq.Error)
	if ok {
		m.Trace("pq.Error: %+v", pqerr)
	} else {
		m.Trace("%T: %+v", err, err)
	}
	message := fmt.Sprintf(msg, args...)
	if err != nil {
		message = fmt.Sprintf("%s Error: %s", message, err.Error())
	}
	ctxErr := m.ctx.Err()
	if ctxErr != nil {
		panic(ctxErr)
	}
	log.Panicf(
		"PANIC: TX: %s PGTXID: %d PGPID: %d message: %s",
		m.id, m.serverTXID, m.serverConnID, message,
	)
}

// Trace logs a message with details about the IDs
// associated with the finalizer
func (m *Finalizer) Trace(format string, args ...interface{}) {
	if !m.TraceFlag {
		return
	}
	message := fmt.Sprintf(format, args...)
	log.Printf(
		"trace: TX: %s PGTXID: %d PGPID: %d message: %s",
		m.id, m.serverTXID, m.serverConnID, message,
	)
}
