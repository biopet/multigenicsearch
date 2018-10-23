organization := "com.github.biopet"
organizationName := "Biopet"

startYear := Some(2018)

name := "MultigenicSearch"
biopetUrlName := "multigenicsearch"

biopetIsTool := true

mainClass in assembly := Some(
  s"nl.biopet.tools.${name.value.toLowerCase()}.${name.value}")

developers += Developer(id = "ffinfo",
                        name = "Peter van 't Hof",
                        email = "pjrvanthof@gmail.com",
                        url = url("https://github.com/ffinfo"))

fork in Test := true

scalaVersion := "2.11.12"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "com.github.biopet" %% "common-utils" % "0.8"
libraryDependencies += "com.github.biopet" %% "spark-utils" % "0.3.1"
libraryDependencies += "com.github.biopet" %% "tool-utils" % "0.6"
libraryDependencies += "com.github.biopet" %% "ngs-utils" % "0.6"
libraryDependencies += "com.github.biopet" %% "tool-test-utils" % "0.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % Provided
