package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/williammoran/txmanager"
	"github.com/williammoran/txmpg"
)

// This example simulates bank transfers between two
// independent databases

/*
SELECT datname, wait_event_type, wait_event, state, query FROM pg_stat_activity;
*/

func main() {
	cs0 := flag.String("0", "", "first database connection")
	cs1 := flag.String("1", "", "second database connection")
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
			randomTransactions(c0, c1)
		}()
	}
	wg.Wait()
}

func makeTable(c *sql.DB) {
	_, err := c.Exec("CREATE TABLE account (id INT, balance NUMERIC)")
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

func randomTransactions(c0, c1 *sql.DB) {
	for i := 0; i < 100; i++ {
		a0 := rand.Intn(5) + 1
		a1 := rand.Intn(5) + 1
		amount := rand.Intn(500) + 1
		transfer(c0, a0, c1, a1, amount)
		a0 = rand.Intn(5) + 1
		a1 = rand.Intn(5) + 1
		amount = rand.Intn(500) + 1
		transfer(c1, a0, c0, a1, amount)
	}
}

func transfer(c0 *sql.DB, a0 int, c1 *sql.DB, a1 int, amount int) {
	fmt.Printf(
		"Start transfer $%d from %d to %d\n",
		amount, a0, a1,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	txm := txmanager.Transaction{}
	f0 := txmpg.MakeFinalizer(ctx, "bank0", c0)
	f0.TraceFlag = true
	f1 := txmpg.MakeFinalizer(ctx, "bank1", c1)
	f1.TraceFlag = true
	txm.Add("bank0", f0)
	txm.Add("bank1", f1)
	var avail int
	err := f0.TX.QueryRowContext(ctx, "SELECT balance FROM account WHERE id = $1 FOR UPDATE", a0).Scan(&avail)
	f0.Trace("Selected balance: %+v\n", err)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			txm.Abort("Context timeout (likely deadlock)")
			return
		}
		panic(err)
	}
	if avail < amount {
		fmt.Printf("Insufficient funds\n")
		txm.Abort("Insufficient funds")
		return
	}
	_, err = f0.TX.ExecContext(ctx, "UPDATE account SET balance = balance - $1 WHERE id = $2", amount, a0)
	f0.Trace("debited balance: %+v\n", err)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			txm.Abort("Context timeout (likely deadlock)")
			return
		}
		panic(err)
	}
	_, err = f1.TX.ExecContext(ctx, "UPDATE account SET balance = balance + $1 WHERE id = $2", amount, a1)
	f1.Trace("Credited balance: %+v\n", err)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			txm.Abort("Context timeout (likely deadlock)")
			return
		}
		panic(err)
	}
	err = txm.Commit()
	if err != nil {
		fmt.Printf("Comitted: %s\n", err.Error())
	} else {
		fmt.Print("Comitted")
	}
	if err != nil {
		panic(err)
	}
	fmt.Printf("Transferred $%d\n", amount)
}
