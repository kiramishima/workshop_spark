package main.scala.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrames {
  def main(args: Array[String]): Unit = {
    // Configurar la sesión de Spark
    val spark = SparkSession.builder()
      .appName("ScalaDataFrames")
      .master("local[*]")
      .getOrCreate()

    try {
      // Probar la conexión imprimiendo la versión de Spark
      println(s"Conexión establecida. Versión de Spark: ${spark.version}")

      // Data
      val data = Seq(("Jinx", 22), ("Solid Snake", 26), ("Naka", 39), ("Marlon", 21),
              ("Jhon Snow", 25), ("Solid Snake", 24), ("Marlon", 19))

      val dataDF = spark.createDataFrame(data).toDF("name", "age")

      val avgDF = dataDF.groupBy("name").agg(avg("age"))
      avgDF.show()

      //
      //get the path to the JSON file
      val jsonFile = args(0)
      //define our schema as before
      val schema = StructType(Array(
        StructField("Id", IntegerType, false),
        StructField("First", StringType, false),
        StructField("Last", StringType, false),
        StructField("Url", StringType, false),
        StructField("Published", StringType, false),
        StructField("Hits", IntegerType, false),
        StructField("Campaigns", ArrayType(StringType), false)
      ))

      //Create a DataFrame by reading from the JSON file a predefined Schema
      val blogsDF = spark.read.schema(schema).json(jsonFile)
      //show the DataFrame schema as output
      blogsDF.show(truncate = false)
      // print the schemas
      println(blogsDF.printSchema)
      println(blogsDF.schema)
      // Show columns and expressions
      blogsDF.select(expr("Hits") * 2).show(2)
      blogsDF.select(col("Hits") * 2).show(2)
      blogsDF.select(expr("Hits * 2")).show(2)
      // show heavy hitters
      blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    } catch {
      case e: Exception => 
        println(s"Error al conectar con Spark: ${e.getMessage}")
    }
  }
}