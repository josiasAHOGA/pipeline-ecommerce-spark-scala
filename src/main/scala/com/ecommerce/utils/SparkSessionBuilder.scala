package com.ecommerce.utils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
object SparkSessionBuilder {
  def build(c: Config): SparkSession = {
    val builder = SparkSession.builder().appName(c.getString("app.name"))
    // spark-submit copie spark.master dans sys.props (JavaMainApplication) et pose SPARK_SUBMIT.
    // Sans ce test, application.conf écraserait un `--master yarn` par local[2].
    val masterDejaImposé =
      sys.props.contains("spark.master") || sys.props.contains("SPARK_SUBMIT")
    if (!masterDejaImposé) builder.master(c.getString("app.spark.master"))
    val spark = builder
      .config("spark.sql.shuffle.partitions", c.getInt("app.spark.shuffle.partitions"))
      .config("spark.sql.session.timeZone", c.getString("app.spark.timezone"))
      .config("spark.sql.autoBroadcastJoinThreshold", c.getLong("app.optimization.auto-broadcast-threshold"))
      .config("spark.sql.adaptive.autoBroadcastJoinThreshold", c.getLong("app.optimization.auto-broadcast-threshold"))
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel(c.getString("app.spark.log-level"))
    spark
  }
}
