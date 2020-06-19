package txmpg

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/williammoran/txmanager"
)

// MakeFinalizer2P is a constructor for a Postgres
// transaction driver
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
	TraceFlag       bool
	name            string
	pool            *sql.DB
	TX              *sql.Tx
	serverTXID      int64
	serverConnID    int64
	id              string
	deferredCommits []func() error
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
	err := m.TX.QueryRow("SELECT txid_status($1)", m.serverTXID).Scan(&status)
	m.Trace("transaction status at Finalize() '%s'", status)
	if status != "in progress" {
		m.panicf("Finalize() not in active transaction", nil)
	}
	m.id = uuid.New().String()
	m.Trace("Create Finalizer2P ID")
	_, err = m.TX.Exec(fmt.Sprintf("PREPARE TRANSACTION '%s'", m.id))
	if err != nil {
		defer func() { m.id = "" }()
		return m.finalizerError(
			txmanager.WrapError(err, "Doing PREPARE"),
		)
	}
	m.Trace("Transaction prepared")
	return nil
}

// Commit finishes the transaction
func (m *Finalizer2P) Commit() {
	var status string
	err := m.TX.QueryRow("SELECT txid_status($1)", m.serverTXID).Scan(&status)
	if err != nil {
		m.panicf("Commit() failed to get txid_status()", err)
	}
	m.Trace("transaction status at Commit() '%s'", status)
	if status == "in progress" {
		err = m.TX.Commit()
		if err != nil {
			m.Trace("Unexpected transaction error! '%s'", err.Error())
			// m.panicf("Failed to commit", err)
		}
	}
	_, err = m.pool.Exec(fmt.Sprintf("COMMIT PREPARED '%s'", m.id))
	if err != nil {
		pqerr := err.(*pq.Error)
		fmt.Printf("COMMIT PREPARED error %+#v", pqerr)
		m.panicf("Failed to commit prepared", err)
	}
	m.Trace("Transaction committed")
}

// Abort rolls back the transaction
func (m *Finalizer2P) Abort() {
	var status string
	err := m.TX.QueryRow("SELECT txid_status($1)", m.serverTXID).Scan(&status)
	m.Trace("transaction status at Abort() '%s'", status)
	if status == "in progress" {
		err = m.TX.Rollback()
		if err != nil {
			m.panicf("Failed to roll back", err)
		}
	}
	if m.id == "" {
		m.Trace("Abort() on transaction that was never finalized")
		return
	}
	_, err = m.pool.Exec(fmt.Sprintf("ROLLBACK PREPARED '%s'", m.id))
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
