package txmpg

import (
	"database/sql"

	"github.com/williammoran/txmanager"
)

// TxFinalizer is an abstraction that allows the single
// phase and 2 phase transaction managers to be used
// interchangeably
type TxFinalizer interface {
	txmanager.TxFinalizer
	PgTx() *sql.Tx
	Trace(format string, args ...interface{})
}
