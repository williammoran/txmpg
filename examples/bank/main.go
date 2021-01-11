package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/williammoran/txmanager"
	"github.com/williammoran/txmpg"
)

// This example simulates bank transfers between two
// independent databases
// Note that it is specifically designed to encounter a
// lot of conflicts and deadlocks to stress test the
// finalizers, so there are more aborts than are likely
// to be encountered in a real world scenario.

/*
SELECT datname, wait_event_type, wait_event, state, query FROM pg_stat_activity;
*/

func main() {
	cs0 := flag.String("0", "", "first database connection")
	cs1 := flag.String("1", "", "second database connection")
	manager := flag.Int("v", 1, "Use either single or 2 phase transactions")
	flag.Parse()
	c0, err := sql.Open("postgres", *cs0)
	if err != nil {
		panic(err)
	}
	c1, err := sql.Open("postgres", *cs1)
	if err != nil {
		panic(err)
	}
	makeTable(c0)
	makeTable(c1)
	addAccounts(c0)
	addAccounts(c1)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			randomTransactions(*manager, c0, c1)
		}()
	}
	wg.Wait()
}

func makeTable(c *sql.DB) {
	_, err := c.Exec("DROP TABLE IF EXISTS account")
	if err != nil {
		panic(err)
	}
	_, err = c.Exec("CREATE TABLE account (id INT, balance NUMERIC)")
	if err != nil {
		panic(err)
	}
}

func addAccounts(c *sql.DB) {
	for i := 1; i < 6; i++ {
		_, err := c.Exec("INSERT INTO account (id, balance) VALUES ($1, $2)", i, 1000)
		if err != nil {
			panic(err)
		}
	}
}

func randomTransactions(manager int, c0, c1 *sql.DB) {
	for i := 0; i < 100; i++ {
		a0 := rand.Intn(5) + 1
		a1 := rand.Intn(5) + 1
		amount := rand.Intn(500) + 1
		transfer(manager, c0, a0, c1, a1, amount)
		a0 = rand.Intn(5) + 1
		a1 = rand.Intn(5) + 1
		amount = rand.Intn(500) + 1
		transfer(manager, c1, a0, c0, a1, amount)
	}
}

func transfer(manager int, c0 *sql.DB, a0 int, c1 *sql.DB, a1 int, amount int) {
	fmt.Printf(
		includeGID("Start transfer $%d from %d to %d\n"),
		amount, a0, a1,
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	txm := txmanager.Transaction{}
	var f0, f1 txmpg.TxFinalizer
	if manager == 1 {
		f0 = txmpg.MakeFinalizer(ctx, "bank0", c0)
		f0.(*txmpg.Finalizer).TraceFlag = true
		f1 = txmpg.MakeFinalizer(ctx, "bank1", c1)
		f1.(*txmpg.Finalizer).TraceFlag = true
	} else {
		f0 = txmpg.MakeFinalizer2P(ctx, "bank0", c0)
		f1 = txmpg.MakeFinalizer2P(ctx, "bank1", c1)
	}
	txm.Add("bank0", f0)
	txm.Add("bank1", f1)
	var avail int
	err := f0.PgTx().QueryRowContext(ctx, "SELECT balance FROM account WHERE id = $1 FOR UPDATE", a0).Scan(&avail)
	f0.Trace(includeGID("Selected balance: %+v\n"), err)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			txm.Abort("Context timeout (likely deadlock)")
			return
		}
		panic(err)
	}
	if avail < amount {
		fmt.Printf(includeGID("Insufficient funds\n"))
		txm.Abort("Insufficient funds")
		return
	}
	_, err = f0.PgTx().ExecContext(ctx, "UPDATE account SET balance = balance - $1 WHERE id = $2", amount, a0)
	f0.Trace(includeGID("debited balance: %+v\n"), err)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			txm.Abort("Context timeout (likely deadlock)")
			return
		}
		panic(err)
	}
	_, err = f1.PgTx().ExecContext(ctx, "UPDATE account SET balance = balance + $1 WHERE id = $2", amount, a1)
	f1.Trace(includeGID("Credited balance: %+v\n"), err)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			txm.Abort("Context timeout (likely deadlock)")
			return
		}
		panic(err)
	}
	defer func() {
		if r := recover(); r != nil {
			if r == context.Canceled || r == context.DeadlineExceeded {
				// TODO: signal that this didn't complete
				return
			}
			panic(r)
		}
	}()
	err = txm.Commit()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			txm.Abort("Context timeout (likely deadlock)")
			return
		}
		panic(err)
	} else {
		fmt.Print(includeGID("Comitted"))
	}
	fmt.Printf(includeGID("Transferred $%d\n"), amount)
}

func includeGID(msg string) string {
	return fmt.Sprintf("GID %d %s", getGID(), msg)
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
