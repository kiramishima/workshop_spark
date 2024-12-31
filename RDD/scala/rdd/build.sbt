import Dependencies._

ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "1.0"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "rdd"

lazy val root = (project in file("."))
  .settings(
    name := "rdd",
    libraryDependencies ++= Seq(
      munit % Test,
      spark_sql
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
