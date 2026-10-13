"""
Query the warehouse in one line.

    py -3.12 include/query.py "select * from mart_category_margin"

Uses Python 3.12 because DuckDB has no prebuilt wheel for 3.15 and will try to
compile from source. Close any DAG run first: DuckDB takes an exclusive file lock.
"""
import sys

import duckdb

con = duckdb.connect("include/warehouse_bhavani.duckdb")
print(con.sql(sys.argv[1]))
con.close()
