# txmpg
Transaction Manager support for PostgreSQL

See also: https://github.com/williammoran/txmanager

This project seeks to build a library that can be used from Golang to reliably manage transactions in PostgreSQL.

The classic example would be two banks on two different
databases that need to transfer funds:
```go
// This code is a simplified version of what's available
// in examples/bank
// The guarantee is that transfer() will complete the
// operations on BOTH accounts, or NEITHER. In no case
// will the operation be only completed on one account.
func transfer(c0 *sql.DB, a0 int, c1 *sql.DB, a1 int, amount int) {
    fmt.Printf(
        "Start transfer $%d from %d to %d\n",
        amount, a0, a1,
    )
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    txm := txmanager.Transaction{}
    // It's always a good idea to defer an Abort(), if
    // the transaction was commited, Abort() is a NOOP
    defer txm.Abort("Defer")
    f0 := txmpg.MakeFinalizer(ctx, "bank0", c0)
    f1 := txmpg.MakeFinalizer(ctx, "bank1", c1)
    txm.Add("bank0", f0)
    txm.Add("bank1", f1)
    var avail int
    err := f0.PgTx().QueryRowContext(ctx, "SELECT balance FROM account WHERE id = $1 FOR UPDATE", a0).Scan(&avail)
    if err != nil {
        panic(err)
    }
    log.Printf("Selected balance = %d\n", avail)
    if avail < amount {
        fmt.Println("Insufficient funds")
        txm.Abort("Insufficient funds")
        panic(err)
    }
    _, err = f0.PgTx().ExecContext(ctx, "UPDATE account SET balance = balance - $1 WHERE id = $2", amount, a0)
    if err != nil {
        panic(err)
    }
    log.Println("debited balance")
    _, err = f1.PgTx().ExecContext(ctx, "UPDATE account SET balance = balance + $1 WHERE id = $2", amount, a1)
    if err != nil {
        panic(err)
    }
    log.Println("Credited balance")
    err = txm.Commit()
    if err != nil {
        panic(err)
    }
    log.Println("Transactions Comitted")
}
```

To try it out, see https://github.com/williammoran/txmpg/tree/master/examples/bank
