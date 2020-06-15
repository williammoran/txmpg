package txmpg

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
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
	return &Finalizer{name: name, TX: tx}
}

// Finalizer manages transactions on a PostgreSQL
// server
type Finalizer struct {
	name            string
	TX              *sql.Tx
	id              string
	deferredCommits []func() error
}

// Defer registers a function to execute a Finalize time
func (m *Finalizer) Defer(exec func() error) {
	m.deferredCommits = append(m.deferredCommits, exec)
}

// Finalize sets up a prepared transaction
func (m *Finalizer) Finalize() error {
	for _, commit := range m.deferredCommits {
		err := commit()
		if err != nil {
			return err
		}
	}
	m.id = uuid.New().String()
	_, err := m.TX.Exec(fmt.Sprintf("PREPARE TRANSACTION '%s'", m.id))
	return err
}

// Commit finishes the transaction
func (m *Finalizer) Commit() {
	_, err := m.TX.Exec(fmt.Sprintf("COMMIT PREPARED '%s'", m.id))
	if err != nil {
		panic(err)
	}
}

// Abort rolls back the transaction
func (m *Finalizer) Abort() {
	_, err := m.TX.Exec(fmt.Sprintf("ROLLBACK PREPARED '%s'", m.id))
	if err != nil {
		panic(err)
	}
}
