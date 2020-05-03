package context

import org.apache.spark.sql.SparkSession

object Context {
  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Reto 3 Curso Spark")
    .master("local[*]")
    .getOrCreate()
}
