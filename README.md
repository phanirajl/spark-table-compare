spark-table-compare
====
# Overview
The intention of this repo is to be used with a DSE Analytics cluster to compare the data in two tables that have the same schema. 
The output of the spark job will be a `csv` file which will contain only a header if there are no differences, or one line for each difference between the two tables.
If any differences are found, the values from both tables will be printed into the `csv` on the same row.
Each column of the `csv` will be prefixed with either `t1` or `t2` corresponding to either `master_table` or `compare_table` respectively so you know which value came from where.
The number of columns in the `csv` will be `(number of columns in t1 + number of columns in t2)`.

# Configuration File
- `system_table` should not need to be changed as it is reading the system schema for the column names.
- `master_table` is for the table name and keyspace of the primary table in the comparison, although since the tables have the same schema there is essentially no difference (one of the tables has to be first, right?).
- `compare_table` is for the table name and keyspace of the second table in the comparison.
- `join_column` is the column from `t1` and `t2` to perform the join.
- `csv_path` is for the output path of the resulting csv file. Best stored on `dsefs` unless testing locally.

# Spark-Submit
- Follow the instructions on [spark-submit](https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/tools/dse/dseSpark-submit.html)
- Submit on the cluster spark master
- add `--driver-memory=`, `--driver-cores=`, `--executor-memory=`, or `--executor-cores=` to `spark-submit` as necessary to support the successful operation of your spark job.
- `spark-submit` should look something like the following: 
```bash
dse spark-submit --files /path/to/application.json \
--conf spark.executor.extraJavaOptions=-Dconfig.file=/path/to/application.json \
--conf spark.driver.extraJavaOptions=-Dconfig.file=/path/to/application.json \
--class "TableCompare" /path/to/spark-table-compare_2.11-0.1.jar
```