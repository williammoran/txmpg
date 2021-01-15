package txmpg

import (
	"database/sql"
	"log"

	"github.com/williammoran/txmanager/v2"
)

// TxFinalizer is an abstraction that allows the single
// phase and 2 phase transaction managers to be used
// interchangeably
type TxFinalizer interface {
	txmanager.TxFinalizer
	PgTx() *sql.Tx
	SetLogger(*log.Logger)
	Trace(format string, args ...interface{})
}
