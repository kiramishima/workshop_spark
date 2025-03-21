from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# definimos nuestro schema
schema = StructType([
  StructField("Id", IntegerType(), False),
  StructField("First", StringType(), False),
  StructField("Last", StringType(), False),
  StructField("Url", StringType(), False),
  StructField("Published", StringType(), False),
  StructField("Hits", IntegerType(), False),
  StructField("Campaigns", ArrayType(StringType()), False)])

# datos
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
      [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
      [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
      [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
      [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
      [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
    ]

# main program
if __name__ == "__main__":
  # Creamos el DataFrame usando la SparkSession
  spark = (SparkSession
    .builder
    .appName("PythonDataFrames")
    .getOrCreate())

  # Creamos el DataFrame 
  blogs_df = spark.createDataFrame(data, schema)
  # mostramos el contenido
  blogs_df.show()
  print()
  # imprime el schema
  print(blogs_df.printSchema())
  # columnas y expresiones
  blogs_df.select(expr("Hits") * 2).show(2)
  blogs_df.select(col("Hits") * 2).show(2)
  blogs_df.select(expr("Hits * 2")).show(2)
  # nueva columna
  blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
  print(blogs_df.schema)