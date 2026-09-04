package com.ecommerce.analytics
import com.ecommerce.models.QualityRow
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class ValidationResult(valid: DataFrame, rejected: DataFrame)
class DataValidation(c: Config) {
  private def number(column: String): Column = col(column).isNotNull && !isnan(col(column)) &&
    abs(col(column)) =!= lit(Double.PositiveInfinity)
  private def split(df: DataFrame, rules: Seq[(Column, String)]): ValidationResult = {
    val reasons = rules.map { case (ok, reason) => when(!coalesce(ok, lit(false)), lit(reason)) }
    val tagged = df.withColumn("rejection_reason", concat_ws("; ", reasons: _*))
    ValidationResult(tagged.filter(col("rejection_reason") === "").drop("rejection_reason"),
      tagged.filter(col("rejection_reason") =!= ""))
  }
  def transactions(df: DataFrame): ValidationResult = {
    val pattern = c.getString("app.validation.transaction.timestamp-pattern")
    val goodDate = udf((s: String) => TimeFeatures.parse(s, pattern).isDefined)
    split(df, Seq(
      (number("amount") && col("amount") > c.getDouble("app.validation.transaction.min-amount-exclusive")) -> "amount non positif ou absent",
      (length(col("timestamp")) === c.getInt("app.validation.transaction.timestamp-length") && goodDate(col("timestamp"))) -> "timestamp invalide"))
  }
  def users(df: DataFrame): ValidationResult = split(df, Seq(
    col("age").between(c.getInt("app.validation.user.min-age"), c.getInt("app.validation.user.max-age")) -> "age hors intervalle ou absent",
    (number("annual_income") && col("annual_income") > c.getDouble("app.validation.user.min-income-exclusive")) -> "annual_income non positif ou absent"))
  def products(df: DataFrame): ValidationResult = split(df, Seq(
    (number("price") && col("price") > c.getDouble("app.validation.product.min-price-exclusive")) -> "price non positif ou absent",
    (number("rating") && col("rating").between(c.getDouble("app.validation.product.min-rating"), c.getDouble("app.validation.product.max-rating"))) -> "rating hors intervalle ou absent"))
  def merchants(df: DataFrame): ValidationResult = split(df, Seq(
    (number("commission_rate") && col("commission_rate").between(c.getDouble("app.validation.merchant.min-commission"), c.getDouble("app.validation.merchant.max-commission"))) -> "commission_rate hors intervalle ou absent"))
  def validateAll(s: Sources): Map[String, ValidationResult] = Map(
    "transactions" -> transactions(s.transactions.toDF), "users" -> users(s.users.toDF),
    "products" -> products(s.products.toDF), "merchants" -> merchants(s.merchants.toDF))
  def orphanCount(tx: DataFrame, ref: DataFrame, key: String): Long =
    tx.join(ref.select(key).distinct(), Seq(key), "left_anti").count()
  def quality(spark: SparkSession, sources: Sources, results: Map[String, ValidationResult]): DataFrame = {
    import spark.implicits._
    val tx = sources.transactions.toDF
    val orphans = (orphanCount(tx, sources.users.toDF, "user_id"),
      orphanCount(tx, sources.products.toDF, "product_id"), orphanCount(tx, sources.merchants.toDF, "merchant_id"))
    sources.frames.map { case (name, raw) =>
      val total = raw.count(); val valid = results(name).valid.count()
      val nulls = raw.select(raw.columns.map(x => sum(when(col(x).isNull, 1L).otherwise(0L)).as(x)): _*)
        .first().toSeq.map(x => Option(x).map(_.asInstanceOf[Long]).getOrElse(0L)).sum
      val o = if (name == "transactions") orphans else (0L, 0L, 0L)
      println(s"VALIDATION $name : $valid valides, ${total-valid} rejetées")
      QualityRow(name,total,valid,total-valid,if(total==0) 0.0 else math.round((total-valid)*10000.0/total)/100.0,
        nulls,o._1,o._2,o._3)
    }.toDF()
  }
}
