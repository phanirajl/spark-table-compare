import org.apache.spark.sql.SparkSession

object TableCompare {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Table Compare").getOrCreate()

    val view1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "columns", "keyspace" -> "system_schema")).load()

    view1.createOrReplaceTempView("columns")

    val df = spark.sql("""
    SELECT concat('t1.', column_name, ' AS t1_', column_name, ', t2.', column_name, ' AS t2_', column_name, ',') AS select_clause_fields
    FROM columns
    WHERE keyspace_name = 'uline_test'
    AND table_name = 'item_by_item_id'
    """)

    val select_clause = df.select("select_clause_fields").rdd.collect.mkString.replace("[", "").replace("]"," ")

    val select_clause_trim = select_clause.substring(0,select_clause.length-2)

    val table1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "item_by_item_id", "keyspace" -> "uline_test")).load()

    val table2 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "item_by_item_id_2", "keyspace" -> "uline_test")).load()

    table1.createOrReplaceTempView("table1")

    table2.createOrReplaceTempView("table2")

    val t1 = spark.sql("SELECT * FROM table1 EXCEPT SELECT * FROM table2")

    val t2 = spark.sql("SELECT * FROM table2 EXCEPT SELECT * FROM table1")

    t1.createOrReplaceTempView("t1")

    t2.createOrReplaceTempView("t2")

    val results = spark.sql("SELECT " + select_clause_trim + " FROM t1 FULL OUTER JOIN t2 ON t1.item_id = t2.item_id")

    results.coalesce(1).write.option("header","true").csv("file:///Users/jamescolvin/Downloads/dse-6.7.5/lib/data/spark/item_by_item_id_diff")
  }
}