package com.ecommerce.analytics
import com.ecommerce.models._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal

case class Sources(transactions: Dataset[Transaction], users: Dataset[User],
  products: Dataset[Product], merchants: Dataset[Merchant]) {
  def frames: Seq[(String, DataFrame)] = Seq("transactions" -> transactions.toDF,
    "users" -> users.toDF, "products" -> products.toDF, "merchants" -> merchants.toDF)
}
class DataIngestion(spark: SparkSession, c: Config) {
  import spark.implicits._
  private def path(name: String): String = c.getString(s"app.data.input.$name")
  private def read[T](name: String)(f: => Dataset[T]): Dataset[T] = try {
    val ds = f
    // Spark est paresseux : count déclenche aussi les erreurs de lecture.
    println(s"LECTURE $name : ${ds.count()} lignes")
    ds
  } catch {
    case NonFatal(e) => throw new IllegalArgumentException(s"Lecture impossible : $name (${path(name)})", e)
  }
  def transactions(): Dataset[Transaction] = read("transactions") {
    spark.read.option("header", true).option("mode", "FAILFAST")
      .schema(Encoders.product[Transaction].schema).csv(path("transactions")).as[Transaction]
  }
  def users(): Dataset[User] = read("users") {
    spark.read.option("mode", "FAILFAST").schema(Encoders.product[User].schema)
      .json(path("users")).as[User]
  }
  def products(): Dataset[Product] = read("products") {
    spark.read.parquet(path("products")).as[Product]
  }
  def merchants(): Dataset[Merchant] = read("merchants") {
    spark.read.option("header", true).option("inferSchema", true).option("mode", "FAILFAST")
      .csv(path("merchants")).withColumn("establishment_date", col("establishment_date").cast("string"))
      .withColumn("commission_rate", col("commission_rate").cast("double")).as[Merchant]
  }
  def loadAll(): Sources = Sources(transactions(), users(), products(), merchants())
}
