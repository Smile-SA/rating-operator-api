#!/bin/sh
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=notasecret
export PGDATABASE=postgres
export POSTGRES_DATABASE_URI=postgresql://localhost:5432/
export PRESTO_DATABASE_URI=presto://root@localhost:8080/hive/metering
export RATING_RATES_DIR=./rates
