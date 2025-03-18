package main.scala.datasets

import org.apache.spark.sql.SparkSession

// case class
case class DeviceIoTData (battery_level: Long, c02_level: Long,
                          cca2: String, cca3: String, cn: String, device_id: Long,
                          device_name: String, humidity: Long, ip: String, latitude: Double,
                          lcd: String, longitude: Double, scale:String, temp: Long,
                          timestamp: Long)

case class DeviceTempByCountry( temp: Long, device_name: String, device_id: Long,
                               cca3: String)
object DataSets {
  def main(args: Array[String]): Unit = {
    // Configurar la sesión de Spark
    val spark = SparkSession.builder()
      .appName("ScalaDataFrames")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val jsonFile = args(0) // iot_devices.json

    val ds = spark.read
      .json(jsonFile)
      .as[DeviceIoTData] // usamos nuestra case class
    ds.show(5, false)

    // filtrando
    val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})
    filterTempDS.show(5, false)

    val dsTemp = ds
      .filter(d => {d.temp > 25})
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]
    dsTemp.show(5, false)

    // usando nombres de columnas
    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
      .where("temp > 25")
      .as[DeviceTempByCountry]

    dsTemp2.show(5, false)

    // Inspeccionando un elemento
    val device = dsTemp.first()
    println("Device:")
    println(device)
  }
}
