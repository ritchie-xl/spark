name := "spark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")