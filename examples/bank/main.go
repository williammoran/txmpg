package main

import (
	"bytes"
	"context"
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

	"github.com/jackc/pgx/v4/pgxpool"
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

func connect(cs string) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgxpool.Connect(ctx, cs)
	if err != nil {
		panic(err)
	}
	return conn
}

func makeTable(c *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.Exec(ctx, "DROP TABLE IF EXISTS account")
	if err != nil {
		panic(err)
	}
	_, err = c.Exec(ctx, "CREATE TABLE account (id INT, balance NUMERIC)")
	if err != nil {
		panic(err)
	}
}

func addAccounts(c *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 1; i < 6; i++ {
		_, err := c.Exec(ctx, "INSERT INTO account (id, balance) VALUES ($1, $2)", i, 1000)
		if err != nil {
			panic(err)
		}
	}
}

func randomTransactions(num, manager int, c0, c1 *pgxpool.Pool, debug bool) {
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

func transfer(manager int, c0 *pgxpool.Pool, a0 int, c1 *pgxpool.Pool, a1 int, amount int, debug bool) {
	fmt.Printf(
		includeGID("Start transfer $%d from %d to %d\n"),
		amount, a0, a1,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 13*time.Second)
	defer cancel()
	txm := txmanager.Transaction{}
	defer txm.Abort("Defer")
	var f0, f1 txmpg.TxFinalizer
	if manager == 1 {
		f0 = txmpg.MakeFinalizer(ctx, "bank0", c0)
		f0.(*txmpg.Finalizer).TraceFlag = debug
		f1 = txmpg.MakeFinalizer(ctx, "bank1", c1)
		f1.(*txmpg.Finalizer).TraceFlag = debug
	} else {
		f0 = txmpg.MakeFinalizer2P(ctx, "bank0", c0)
		f0.(*txmpg.Finalizer2P).TraceFlag = debug
		f1 = txmpg.MakeFinalizer2P(ctx, "bank1", c1)
		f1.(*txmpg.Finalizer2P).TraceFlag = debug
	}
	txm.Add("bank0", f0)
	txm.Add("bank1", f1)
	var avail int
	err := f0.PgTx().QueryRow(ctx, "SELECT balance FROM account WHERE id = $1 FOR UPDATE", a0).Scan(&avail)
	f0.Trace(includeGID("Selected balance = %d err = %+v\n"), avail, err)
	if err != nil {
		return
	}
	if avail < amount {
		fmt.Println(includeGID("Insufficient funds"))
		txm.Abort("Insufficient funds")
		return
	}
	_, err = f0.PgTx().Exec(ctx, "UPDATE account SET balance = balance - $1 WHERE id = $2", amount, a0)
	f0.Trace(includeGID("debited balance, err = %+v\n"), err)
	if err != nil {
		return
	}
	_, err = f1.PgTx().Exec(ctx, "UPDATE account SET balance = balance + $1 WHERE id = $2", amount, a1)
	f1.Trace(includeGID("Credited balance, err = %+v\n"), err)
	if err != nil {
		return
	}
	err = txm.Commit()
	if err == nil {
		fmt.Printf(includeGID("Commited transfer of $%d\n"), amount)
	}
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
