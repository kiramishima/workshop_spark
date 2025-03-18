from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Creamos el DataFrame usando la SparkSession
spark = (SparkSession
  .builder
  .appName("PythonDataFrames")
  .getOrCreate())

# Creamos el DataFrame 
data_df = spark.createDataFrame([("Jinx", 22), ("Solid Snake", 26), ("Naka", 39),
    ("Marlon", 21), ("Jhon Snow", 25), ("Solid Snake", 24), ("Marlon", 19)], ["name", "age"])
# Agrupamos por nombre y aplicamos la agregación de la media de la edad (Age)
avg_df = data_df.groupBy("name").agg(avg("age"))
# Mostramos el resultado
avg_df.show()