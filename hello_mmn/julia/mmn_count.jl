using Spark
import Spark: appName, master, getOrCreate
using DataFrames

ENV["JULIA_COPY_STACKS"] = 1
ENV["BUILD_SPARK_VERSION"] = "3.5.3" 
ENV["SPARK_SUBMIT_OPTS"] = "--add-opens java.base/sun.nio.ch=ALL-UNNAMED"

function main(args::Vector{String})
    if length(args) < 1
        println("Usage: MnMcount <mnm_file_dataset>")
        exit(1)
    end

    # Crear una SparkSession
    builder = SparkSession.builder
    builder = appName(builder, "MnMCount")
    builder = master(builder, "local")
    spark = getOrCreate(builder)
    # spark = SparkSession.builder.appName("MnMCount").master("local").getOrCreate()
    # Obtener el nombre del archivo del conjunto de datos de M&M
    mnmFile = args[1]

    # Leer el archivo en un DataFrame de Spark
    mnmDF = read_csv(spark, mnmFile, header=true, inferSchema=true)

    # Mostrar el DataFrame
    show(mnmDF, 5, false)

    # Agregar el conteo de todos los colores y agrupar por estado y color
    # Ordenar en orden descendente
    countMnMDF = select(mnmDF, "State", "Color", "Count") |>
                 groupby(["State", "Color"]) |>
                 aggregate(sum("Count")) |>
                 orderby(desc("sum(Count)"))

    # Mostrar toda la agregación resultante para todas las fechas y colores
    show(countMnMDF, 60)
    println("Total Rows = $(count(countMnMDF))")
    println()

    # Encontrar el conteo agregado para California filtrando
    caCountMnNDF = filter(row -> row["State"] == "CA", mnmDF) |>
                   groupby(["State", "Color"]) |>
                   aggregate(sum("Count")) |>
                   orderby(desc("sum(Count)"))

    # Mostrar la agregación resultante para California
    show(caCountMnNDF, 10)
end

# Llamar a la función main con los argumentos de la línea de comandos
main(ARGS)
