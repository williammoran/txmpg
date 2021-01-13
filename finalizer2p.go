package txmpg

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/williammoran/txmanager"
)

// MakeFinalizer2P is a constructor for a Postgres
// transaction driver that uses 2-phase commit
func MakeFinalizer2P(
	ctx context.Context, name string, cPool *pgxpool.Pool,
) *Finalizer2P {
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
	finalizer := Finalizer2P{
		ctx:          ctx,
		pool:         cPool,
		name:         name,
		TX:           tx,
		serverTXID:   id,
		serverConnID: pid,
	}
	return &finalizer
}

// Finalizer2P manages transactions on a PostgreSQL
// server
type Finalizer2P struct {
	ctx             context.Context
	TraceFlag       bool
	name            string
	pool            *pgxpool.Pool
	TX              pgx.Tx
	serverTXID      int64
	serverConnID    int64
	id              string
	deferredCommits []func() error
	comitted        bool
	aborted         bool
}

// PgTx returns the underlying transaction object
func (m *Finalizer2P) PgTx() pgx.Tx {
	return m.TX
}

// Defer registers a function to execute at Finalize time
func (m *Finalizer2P) Defer(exec func() error) {
	m.Trace("Defer()")
	m.deferredCommits = append(m.deferredCommits, exec)
}

// Finalize sets up a prepared transaction
func (m *Finalizer2P) Finalize() error {
	for _, commit := range m.deferredCommits {
		err := commit()
		if err != nil {
			return m.finalizerError(
				txmanager.WrapError(
					err, "Running deferred commits",
				))
		}
	}
	var status string
	err := m.pool.QueryRow(m.ctx, "SELECT txid_status($1)", m.serverTXID).Scan(&status)
	if err != nil {
		m.panicf("Getting txid_status()", err)
	}
	m.Trace("transaction status before PREPARE '%s'", status)
	if status != "in progress" {
		ctxErr := m.ctx.Err()
		if ctxErr == context.DeadlineExceeded || ctxErr == context.Canceled {
			return ctxErr
		}
		return errors.New("Finalize() not in active transaction")
	}
	m.id = uuid.New().String()
	_, err = m.TX.Exec(m.ctx, fmt.Sprintf("PREPARE TRANSACTION '%s'", m.id))
	if err != nil {
		defer func() { m.id = "" }()
		return m.finalizerError(
			txmanager.WrapError(err, "Doing PREPARE"),
		)
	}
	m.Trace("Transaction prepared")
	err = m.TX.Commit(m.ctx)
	if err != nil {
		m.panicf("Commit()", err)
	}
	err = m.pool.QueryRow(m.ctx, "SELECT txid_status($1)", m.serverTXID).Scan(&status)
	if err != nil {
		m.panicf("Getting txid_status()", err)
	}
	m.Trace("transaction status after PREPARE '%s'", status)
	return nil
}

// Commit finishes the transaction
func (m *Finalizer2P) Commit() error {
	if m.id == "" {
		return errors.New("Commit() on transaction that was never finalized")
	}
	if m.comitted {
		return errors.New("Commit() on already comitted transation")
	}
	_, err := m.pool.Exec(m.ctx, fmt.Sprintf("COMMIT PREPARED '%s'", m.id))
	if err != nil {
		m.Trace("COMMIT PREPARED error: %s", err.Error())
		ctxErr := m.ctx.Err()
		if ctxErr != nil {
			return ctxErr
		}
		return txmanager.WrapError(err, "Failed to commit prepared")
	}
	m.comitted = true
	m.Trace("Transaction committed")
	return nil
}

// Abort rolls back the transaction
func (m *Finalizer2P) Abort() {
	if m.aborted {
		m.Trace("Abort() on already aborted transaction")
		return
	}
	if m.comitted {
		m.Trace("Abort() on comitted transaction")
		return
	}
	if m.id == "" {
		m.Trace("Abort() on transaction that was never finalized")
		m.aborted = true
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := m.pool.Exec(ctx, fmt.Sprintf("ROLLBACK PREPARED '%s'", m.id))
	if err != nil {
		m.panicf("Failed ROLLBACK PREPARED", err)
	}
	m.aborted = true
	m.Trace("ROLLBACK PREPARED")
}

const msgformat = "%s: CONN: %s TX: %s PGTXID: %d PGPID: %d message: %s"

func (m *Finalizer2P) finalizerError(err error) *txmanager.Error {
	return txmanager.WrapError(
		err,
		fmt.Sprintf(
			msgformat,
			"Error", m.name, m.id, m.serverTXID, m.serverConnID, err.Error(),
		),
	)
}

func (m *Finalizer2P) panicf(msg string, err error, args ...interface{}) {
	_, f, l, _ := runtime.Caller(1)
	log.Printf(
		msgformat,
		"PANIC", m.name, m.id, m.serverTXID, m.serverConnID,
		fmt.Sprintf("called from %s:%d", f, l),
	)
	message := fmt.Sprintf(msg, args...)
	if err != nil {
		message = fmt.Sprintf("%s Error: %s", message, err.Error())
	}
	log.Panicf(
		msgformat,
		"PANIC", m.name, m.id, m.serverTXID, m.serverConnID, message,
	)
}

// Trace logs a message with details about the IDs
// associated with the finalizer
func (m *Finalizer2P) Trace(format string, args ...interface{}) {
	if !m.TraceFlag {
		return
	}
	message := fmt.Sprintf(format, args...)
	log.Printf(
		msgformat,
		"trace", m.name, m.id, m.serverTXID, m.serverConnID, message,
	)
}
