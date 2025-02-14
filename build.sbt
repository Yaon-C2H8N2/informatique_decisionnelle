// Project name and version
name := "informatique_decisionnelle"
version := "0.1.0"

// Scala version
scalaVersion := "2.12.17"

// Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.postgresql" % "postgresql" % "42.7.2",
  "org.apache.activemq" % "activemq-all" % "5.17.5"
)

// Add a repository for Spark dependencies (optional if not in default repos)
resolvers += "Apache Spark Repository" at "https://repo1.maven.org/maven2/"