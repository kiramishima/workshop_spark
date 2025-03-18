import sbt._

object Dependencies {
  val spark_version = "3.5.5"
  lazy val munit = "org.scalameta" %% "munit" % "1.1.0"
  lazy val spark_core = "org.apache.spark" %% "spark-core" % spark_version
  lazy val spark_sql = "org.apache.spark" %% "spark-sql" % spark_version % "provided"
}
