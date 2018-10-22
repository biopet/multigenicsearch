organization := "com.github.biopet"
organizationName := "Biopet"

//TODO: Start year should reflect the tools original start year on github.com/biopet/biopet in the tools section
startYear := Some(2017)

name := "MultigenicSearch"
biopetUrlName := "multigenicsearch"

biopetIsTool := true

mainClass in assembly := Some(
  s"nl.biopet.tools.${name.value.toLowerCase()}.${name.value}")

developers := List(
  Developer(id = "ffinfo",
            name = "Peter van 't Hof",
            email = "pjrvanthof@gmail.com",
            url = url("https://github.com/ffinfo")),
  Developer(id = "rhpvorderman",
            name = "Ruben Vorderman",
            email = "r.h.p.vorderman@lumc.nl",
            url = url("https://github.com/rhpvorderman"))
)

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12", "2.12.6")

libraryDependencies += "com.github.biopet" %% "tool-utils" % "0.6"
libraryDependencies += "com.github.biopet" %% "tool-test-utils" % "0.3"
