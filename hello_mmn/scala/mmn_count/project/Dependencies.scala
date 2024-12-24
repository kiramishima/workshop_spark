import sbt._

object Dependencies {
  lazy val munit = "org.scalameta" %% "munit" % "0.7.29"
  lazy val spark_core = "org.apache.spark" %% "spark-core" % "3.5.3"
  lazy val spark_sql = "org.apache.spark" %% "spark-sql" % "3.5.3" % "provided"

}
