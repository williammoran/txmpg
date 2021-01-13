package txmpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/williammoran/txmanager"
)

// MakeFinalizer2P is a constructor for a Postgres
// transaction driver that uses 2-phase commit
func MakeFinalizer2P(
	ctx context.Context, name string, cPool *sql.DB,
) *Finalizer2P {
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
	pool            *sql.DB
	TX              *sql.Tx
	serverTXID      int64
	serverConnID    int64
	id              string
	deferredCommits []func() error
}

// PgTx returns the underlying SQL transaction object
func (m *Finalizer2P) PgTx() *sql.Tx {
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
	m.id = uuid.New().String()
	m.Trace("Create Finalizer2P ID")
	_, err := m.TX.Exec(fmt.Sprintf("PREPARE TRANSACTION '%s'", m.id))
	if err != nil {
		defer func() { m.id = "" }()
		return m.finalizerError(
			txmanager.WrapError(err, "Doing PREPARE"),
		)
	}
	m.Trace("Transaction prepared")
	m.TX = nil
	return nil
}

// Commit finishes the transaction
func (m *Finalizer2P) Commit() error {
	if m.TX != nil {
		return errors.New("Commit on non-finalized transaction")
	}
	_, err := m.pool.Exec(fmt.Sprintf("COMMIT PREPARED '%s'", m.id))
	if err != nil {
		m.Trace("COMMIT PREPARED error: %s", err.Error())
		pqerr, casted := err.(*pq.Error)
		if casted {
			m.Trace("COMMIT PREPARED error %+#v", pqerr)
		}
		ctxErr := m.ctx.Err()
		if ctxErr != nil {
			return ctxErr
		}
		return txmanager.WrapError(err, "Failed to commit prepared")
	}
	m.Trace("Transaction committed")
	return nil
}

// Abort rolls back the transaction
func (m *Finalizer2P) Abort() {
	if m.TX != nil {
		m.Trace("Abort() doing TX.Rollback()")
		err := m.TX.Rollback()
		if err != nil {
			if err != sql.ErrTxDone {
				m.panicf("Failed Rollback()", err)
			}
			m.Trace("Abort() on failed transaction")
		}
		return
	}
	if m.id == "" {
		m.Trace("Abort() on transaction that was never finalized")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := m.pool.ExecContext(ctx, fmt.Sprintf("ROLLBACK PREPARED '%s'", m.id))
	if err != nil {
		m.panicf("Failed ROLLBACK PREPARED", err)
	}
	m.Trace("ROLLBACK PREPARED")
}

func (m *Finalizer2P) finalizerError(err error) *txmanager.Error {
	return txmanager.WrapError(
		err,
		fmt.Sprintf(
			"TX: %s PGTXID: %d PGPID: %d message: %s",
			m.id, m.serverTXID, m.serverConnID, err.Error(),
		),
	)
}

func (m *Finalizer2P) panicf(msg string, err error, args ...interface{}) {
	_, f, l, _ := runtime.Caller(1)
	log.Printf("panicf called from %s:%d", f, l)
	message := fmt.Sprintf(msg, args...)
	if err != nil {
		message = fmt.Sprintf("%s Error: %s", message, err.Error())
	}
	log.Panicf(
		"TX: %s PGTXID: %d PGPID: %d message: %s",
		m.id, m.serverTXID, m.serverConnID, message,
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
		"TX: %s PGTXID: %d PGPID: %d message: %s",
		m.id, m.serverTXID, m.serverConnID, message,
	)
}
