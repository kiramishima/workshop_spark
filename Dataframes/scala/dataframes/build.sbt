val scala3Version = "2.12.18"
val spark_version = "3.5.4"

lazy val root = project
  .in(file("."))
  .settings(
    name := "dataframes",
    version := "0.1.0",
    organization := "com.example",
    organizationName := "dataframes",
    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.apache.spark" %% "spark-core" % spark_version,
      "org.apache.spark" %% "spark-sql" % spark_version % "provided"
    )
  )
