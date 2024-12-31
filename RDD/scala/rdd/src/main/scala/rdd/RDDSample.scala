package main.scala.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDSample {
    def main(args: Array[String]): Unit = {
        // Crear una configuración de Spark
        val conf = new SparkConf().setAppName("ScalaRDD").setMaster("local[2]")
        // Crear una SparkContext
        val sc = new SparkContext(conf)
        // Crear un RDD vacio
        val rdd_vacio = sc.emptyRDD[Int]
        println(s"Elementos del RDD: ${rdd_vacio.count()}")
        println(s"Particiones del RDD: ${rdd_vacio.getNumPartitions}")

        // Crear un RDD con parallelize
        val rdd_vacio_3p = sc.parallelize[Any](Seq.empty[Any], 3)
        println(s"Elementos en el RDD: ${rdd_vacio_3p.count()}")
        println(s"Particiones en el RDD: ${rdd_vacio_3p.getNumPartitions}")


        // Crear un RDD de tuplas (nombre, edad)
        val dataRDD = sc.parallelize[Tuple2[String, Int]](
            Seq(("Jinx", 22), ("Solid Snake", 26), ("Naka", 39), ("Marlon", 21),
                ("Jhon Snow", 25), ("Solid Snake", 24), ("Marlon", 19)))
        println(s"Elementos en dataRDD: ${dataRDD.count()}")
        println(s"Particiones en dataRDD: ${dataRDD.getNumPartitions}")
        // Usaremos map & reduceByKey y obtener la media de la edad
        val edadesRDD = (dataRDD
          .map { case (name, age) => (name, (age, 1)) }
          .reduceByKey { case ((age1, count1), (age2, count2)) => (age1 + age2, count1 + count2) }
        )
        // Accion
        edadesRDD.collect().foreach(println)

        val r2 = edadesRDD.map{ case (name, (totalAge, count)) => (name, totalAge.toDouble / count) }
        r2.collect().foreach(println)

        // Guardar
        r2.saveAsTextFile("D:\\Workspace\\ws_python\\workshop_spark\\RDD\\scala\\scala-rdd")
        r2.coalesce(1).saveAsTextFile("D:\\Workspace\\ws_python\\workshop_spark\\RDD\\scala\\coalesce-rdd")
        // Detener la SparkContext
        sc.stop()
    }
}