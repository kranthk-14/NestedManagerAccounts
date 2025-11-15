name := "NestedManagerAccountsETL"

version := "2.0.0"

scalaVersion := "2.12.17"

val sparkVersion = "3.4.0"
val redshiftVersion = "2.1.0.9"

libraryDependencies ++= Seq(
  // Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  
  // Database connectivity
  "com.amazon.redshift" % "redshift-jdbc42" % redshiftVersion,
  "org.postgresql" % "postgresql" % "42.6.0",
  
  // Configuration and utilities
  "com.typesafe" % "config" % "1.4.2",
  "com.github.scopt" %% "scopt" % "4.1.0",
  
  // Testing dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
)

// Assembly settings for creating a fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("application.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Compiler options
scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused"
)

// Test settings
Test / parallelExecution := false
Test / fork := true

// Runtime settings
run / fork := true
run / connectInput := true

// Enable sbt-assembly plugin
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")

// Project organization
organization := "com.amazon.ads"
organizationName := "Amazon Advertising"

// Java compatibility
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// Exclude Spark dependencies from assembly (they should be provided by the cluster)
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter { jar =>
    jar.data.getName.contains("spark-") ||
    jar.data.getName.contains("scala-library") ||
    jar.data.getName.contains("hadoop-")
  }
}

// Main class for running
Compile / mainClass := Some("com.amazon.ads.etl.nestedmanageraccounts.NestedManagerAccountsETL")

// Resource directories
Compile / resourceDirectories += baseDirectory.value / "conf"

// Environment-specific configurations
lazy val Dev = config("dev") extend Compile
lazy val Prod = config("prod") extend Compile

// Custom tasks
lazy val runDev = taskKey[Unit]("Run application in development mode")
runDev := {
  (Compile / runMain).toTask(" com.amazon.ads.etl.nestedmanageraccounts.NestedManagerAccountsETL --debug true --save-intermediate true").value
}

lazy val runProd = taskKey[Unit]("Run application in production mode") 
runProd := {
  (Compile / runMain).toTask(" com.amazon.ads.etl.nestedmanageraccounts.NestedManagerAccountsETL").value
}

// Documentation settings
autoAPIMappings := true
apiURL := Some(url("https://spark.apache.org/docs/latest/api/scala/"))

// Publishing settings (if needed for internal repositories)
publishTo := Some("Internal Repository" at "https://your-internal-repo.com/repository/maven-public/")
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

// Dependency resolution
resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
  "Redshift Repository" at "https://s3.amazonaws.com/redshift-maven-repository/release"
)

// Plugin requirements
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7")

// Build info
ThisBuild / version := "2.0.0"
ThisBuild / scalaVersion := "2.12.17"
