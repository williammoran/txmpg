# txmpg
Transaction Manager support for PostgreSQL

See also: https://github.com/williammoran/txmanager

This project seeks to build a library that can be used from Golang to reliably manage transactions in PostgreSQL.

It currently works with standard transactions, but the 2-phase commit support is buggy.

To try it out, see https://github.com/williammoran/txmpg/tree/master/examples/bank
