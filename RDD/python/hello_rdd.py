from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("PythonRDD")

# Inicializo el Spark Context
sc = SparkContext(conf = conf)

# Crear un RDD vacio
rdd_vacio = sc.emptyRDD()
print(f"Elementos en el RDD: {rdd_vacio.count()}")
print(f"Particiones en el RDD: {rdd_vacio.getNumPartitions()}")

# Crear un RDD con parallelize
rdd_vacio_3p = sc.parallelize([], 3)

print(f"Elementos en el RDD: {rdd_vacio_3p.count()}")
print(f"Particiones en el RDD: {rdd_vacio_3p.getNumPartitions()}")

# Crear un RDD de tuplas (nombre, edad)
dataRDD = sc.parallelize([("Jinx", 22), ("Solid Snake", 26),
                        ("Naka", 39), ("Marlon", 21), ("Jhon Snow", 25),
                        ("Solid Snake", 24), ("Marlon", 19)])
print(f"Elementos en dataRDD: {dataRDD.count()}")
print(f"Particiones en dataRDD: {dataRDD.getNumPartitions()}")
# Usaremos map & reduceByKey y obtener la media de la edad
edadesRDD = (dataRDD
    .map(lambda x: (x[0], (x[1], 1)))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    .map(lambda x: (x[0], x[1][0]/x[1][1]))
)
# Accion
result = edadesRDD.collect()
print(result)

r2 = edadesRDD.map(lambda x: (x[0], x[1][0]/x[1][1]))
print(r2.collect())


# Guardamos el RDD
r2.saveAsTextFile('./python/python-rdd')
r2.coalesce(1).saveAsTextFile('./python/coalesce-rdd')
sc.stop()