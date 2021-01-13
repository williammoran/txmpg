package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/williammoran/txmanager"
	"github.com/williammoran/txmpg"
)

// This example simulates bank transfers between two
// independent databases
// Note that it is specifically designed to encounter a
// lot of conflicts and deadlocks to stress test the
// finalizers, so there are more aborts than are likely
// to be encountered in a real world scenario.

// Some example command lines:
// All assume that databases called "bank0" and "bank1"
// already exist on a PostgreSQL server running locally:
// * Run the standard finalizer with 5 concurrent connections
//   executing 100 transfers each (the defaults)
// ./bank -0 "user=postgres dbname=bank0" -1 "user=postgres dbname=bank1"
// * Run the 2-phase finalizer with 3 concurrent connections
//   executing 200 transfers each
// ./bank -0 "user=postgres dbname=bank0" -1 "user=postgres dbname=bank1" -v 2 -c 3 -t 200
// * Run the 2-phase finalizer with verbose output
// ./bank -0 "user=postgres dbname=bank0" -1 "user=postgres dbname=bank1" -v 2 -d

func main() {
	cs0 := flag.String("0", "", "first database connection")
	cs1 := flag.String("1", "", "second database connection")
	manager := flag.Int("v", 1, "Use either single or 2 phase transactions")
	routines := flag.Int("c", 5, "Number of concurrent routines")
	transactions := flag.Int("t", 100, "Number of transactions per goroutine")
	debug := flag.Bool("d", false, "Enable transaction tracing")
	flag.Parse()
	c0 := connect(*cs0)
	defer c0.Close()
	c1 := connect(*cs1)
	defer c1.Close()
	makeTable(c0)
	makeTable(c1)
	addAccounts(c0)
	addAccounts(c1)
	// CTRL+\ from the terminal while this is running will
	// produce a full stack trace, which can be interesting
	// when the code stalls due to deadlocks.
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			stacklen := runtime.Stack(buf, true)
			log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < *routines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			randomTransactions(*transactions, *manager, c0, c1, *debug)
		}()
	}
	wg.Wait()
}

// connect just connects using the passed conntection
// string or panic()
func connect(cs string) *sql.DB {
	conn, err := sql.Open("postgres", cs)
	if err != nil {
		panic(err)
	}
	return conn
}

// makeTable drops any table called "accounts" then
// creates it with a simple account ID/balance structure
func makeTable(c *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.ExecContext(ctx, "DROP TABLE IF EXISTS account")
	if err != nil {
		panic(err)
	}
	_, err = c.ExecContext(ctx, "CREATE TABLE account (id INT, balance NUMERIC)")
	if err != nil {
		panic(err)
	}
}

// addAccounts adds 5 accounts with $1000 each
func addAccounts(c *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 1; i < 6; i++ {
		_, err := c.ExecContext(ctx, "INSERT INTO account (id, balance) VALUES ($1, $2)", i, 1000)
		if err != nil {
			panic(err)
		}
	}
}

// randomeTransaction executes the number of transactions
// specified by num using on the databases specified by
// c0 and c1. 1/2 the transactions transfer from c0 -> c1
// and half from c1 -> c0
func randomTransactions(num, manager int, c0, c1 *sql.DB, debug bool) {
	fmt.Println(includeGID("Starting transaction thread"))
	for i := 0; i < num/2; i++ {
		a0 := rand.Intn(5) + 1
		a1 := rand.Intn(5) + 1
		amount := rand.Intn(500) + 1
		transfer(manager, c0, a0, c1, a1, amount, debug)
		a0 = rand.Intn(5) + 1
		a1 = rand.Intn(5) + 1
		amount = rand.Intn(500) + 1
		transfer(manager, c1, a0, c0, a1, amount, debug)
	}
}

// transfer does a single transfer of funds between the
// specified accounts
func transfer(manager int, c0 *sql.DB, a0 int, c1 *sql.DB, a1 int, amount int, debug bool) {
	fmt.Printf(
		includeGID("Start transfer $%d from %d to %d\n"),
		amount, a0, a1,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	txm := txmanager.Transaction{}
	// It's always a good idea to defer an Abort(), if
	// the transaction was commited, Abort() is a NOOP
	defer txm.Abort("Defer")
	var f0, f1 txmpg.TxFinalizer
	if manager == 1 {
		f0 = txmpg.NewFinalizer(ctx, "bank0", c0)
		f0.(*txmpg.Finalizer).TraceFlag = debug
		f1 = txmpg.NewFinalizer(ctx, "bank1", c1)
		f1.(*txmpg.Finalizer).TraceFlag = debug
	} else {
		f0 = txmpg.NewFinalizer2P(ctx, "bank0", c0)
		f0.(*txmpg.Finalizer2P).TraceFlag = debug
		f1 = txmpg.NewFinalizer2P(ctx, "bank1", c1)
		f1.(*txmpg.Finalizer2P).TraceFlag = debug
	}
	txm.Add("bank0", f0)
	txm.Add("bank1", f1)
	var avail int
	err := f0.PgTx().QueryRowContext(ctx, "SELECT balance FROM account WHERE id = $1 FOR UPDATE", a0).Scan(&avail)
	f0.Trace(includeGID("Selected balance = %d err = %+v\n"), avail, err)
	if err != nil {
		return
	}
	if avail < amount {
		fmt.Println(includeGID("Insufficient funds"))
		txm.Abort("Insufficient funds")
		return
	}
	_, err = f0.PgTx().ExecContext(ctx, "UPDATE account SET balance = balance - $1 WHERE id = $2", amount, a0)
	f0.Trace(includeGID("debited balance, err = %+v\n"), err)
	if err != nil {
		return
	}
	_, err = f1.PgTx().ExecContext(ctx, "UPDATE account SET balance = balance + $1 WHERE id = $2", amount, a1)
	f1.Trace(includeGID("Credited balance, err = %+v\n"), err)
	if err != nil {
		return
	}
	err = txm.Commit()
	if err == nil {
		fmt.Printf(includeGID("Commited transfer of $%d\n"), amount)
	}
}

// includeGID adds the goroutine ID to the beginning of
// a string. Since this example is specifically for
// concurrency, it can be helpful to track which thread
// is doing which operations
func includeGID(msg string) string {
	return fmt.Sprintf("GID %d %s", getGID(), msg)
}

// getGID just returns the goroutine ID, which is
// a surprisingly complex operation
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
