ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"
ThisBuild / organization := "com.ecommerce"
name := "EcommerceAnalytics"
val sparkVersion = "3.5.6"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.typesafe" % "config" % "1.4.3"
)
Compile / mainClass := Some("com.ecommerce.analytics.MainApp")
Compile / run / fork := true
Test / fork := true
Test / parallelExecution := false
// Spark est « provided » : spark-submit l'apporte, l'assembly ne l'embarque pas.
// sbt run / runMain doivent pourtant voir Spark, d'où le classpath Compile (qui inclut provided).
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated
javaOptions ++= Seq("-Xmx3g", "-Duser.timezone=UTC", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
javaOptions ++= Seq("java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.cs", "sun.security.action", "sun.util.calendar").map(p => s"--add-opens=java.base/$p=ALL-UNNAMED")
// winutils : sans cela, sbt test échoue sous Windows à l'écriture Hadoop (dashboard.html).
javaOptions ++= {
  val hadoop = baseDirectory.value / ".runtime" / "hadoop"
  if (hadoop.exists) Seq("-Dhadoop.home.dir=" + hadoop.getAbsolutePath) else Seq.empty
}
envVars ++= {
  val hadoop = baseDirectory.value / ".runtime" / "hadoop"
  if (hadoop.exists) Map("HADOOP_HOME" -> hadoop.getAbsolutePath) else Map.empty
}
Test / test := (Test / runMain).toTask(" com.ecommerce.RegressionSuite").value
assembly / mainClass := Some("com.ecommerce.analytics.MainApp")
assembly / assemblyJarName := "ecommerce-analytics.jar"
assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)
// Le classpath local inclut Spark pour sbt run ; l'assembly ne l'embarque jamais.
assembly / assemblyExcludedJars := (assembly / fullClasspath).value.filter { entry =>
  entry.data.isFile && entry.data.getName != "config-1.4.3.jar"
}
assembly / test := {}
