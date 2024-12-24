from __future__ import print_function

import sys
from pyspark.sql import SparkSession
import time

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("PythonMnMCount") # Nombre de la aplicacion
        .getOrCreate())
    # nombre del archivo
    mnm_file = sys.argv[1]
    # leemos el archivo en un DataFrame de Spark 
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))
    # mostramos las primeras 5 lineas
    mnm_df.show(n=5, truncate=False)

    # agregamos un conteo de los colores y agrupamos por estado y color
    # ordenamos de manera descendente
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # mostramos todos los resultado de la agregación para todas las fechas y colores
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # buscamos los registros para California y realizamos la agregación
    ca_count_mnm_df = (mnm_df.select("*")
                    .where(mnm_df.State == 'CA')
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # mostramos los resultados
    ca_count_mnm_df.show(n=10, truncate=False)

    time.sleep(70)