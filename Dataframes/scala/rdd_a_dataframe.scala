//> using scala "3.6.3"
//> using repository "central"
//> using dep "org.apache.spark::spark-sql:3.5.4"

import org.apache.spark.sql.SparkSession

@main def connectToSpark(): Unit = {
  // Configurar la sesión de Spark
  val spark = SparkSession.builder()
    .appName("SparkConnectionTest")
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
  } catch {
    case e: Exception => 
      println(s"Error al conectar con Spark: ${e.getMessage}")
  } finally {
    // Cerrar la sesión de Spark
    spark.stop()
  }
}
