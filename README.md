# spark-table-compare
The intention of this repo is to be used with a DSE Analytics cluster to compare the data in two tables that have the same schema.

## Setup
- Modify the `val df = spark.sql` statement where `keyspace_name` and `table_name` is your specific "master" table.
- Modify `table1 = spark.read.format` statement where `keyspace` and `table` is your specific "master" table.
- Modify `table2 = spark.read.format` statement where `keyspace` and `table` is your the table to be compared.
- Modify the last line, `results.coalesce` to a file location, ideally on `dsefs:///`

## Spark-Submit
- Follow the instructions on [spark-submit](https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/tools/dse/dseSpark-submit.html)
- Submit on the cluster spark master
- `dse spark-submit --class "TableCompare" ./spark-table-compare_2.11-0.1.jar` 