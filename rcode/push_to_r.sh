#!/bin/sh

psql -h 10.208.97.116 -p 5432 etl analytics -f "sql/pve_non_pride_viewed_count.sql"

