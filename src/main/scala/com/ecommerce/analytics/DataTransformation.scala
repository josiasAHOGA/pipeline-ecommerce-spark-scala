package com.ecommerce.analytics
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class Enrichment(data: DataFrame, rejected: DataFrame)
class DataTransformation(c: Config) {
  private def small(df: DataFrame, optimized: Boolean): DataFrame =
    if(optimized && c.getBoolean("app.optimization.enable-broadcast")) broadcast(df) else df
  def enrichTransactionData(v: Map[String, ValidationResult], optimized: Boolean): Enrichment = {
    val users = v("users").valid.withColumn("_user_found",lit(true))
    val products = v("products").valid.select(col("product_id"),col("name").as("product_name"),
      col("category").as("product_category"),col("price"),col("rating"),col("stock"),
      col("merchant_id").as("catalog_merchant_id")).withColumn("_product_found",lit(true))
    val merchants = v("merchants").valid.select(col("merchant_id"),col("name").as("merchant_name"),
      col("category").as("merchant_category"),col("region"),col("commission_rate"))
      .withColumn("_merchant_found",lit(true))
    // Les left joins permettent de conserver et expliquer chaque rejet avant exclusion.
    val joined = v("transactions").valid.join(users,Seq("user_id"),"left")
      .join(products,Seq("product_id"),"left")
      .join(small(merchants,optimized),Seq("merchant_id"),"left")
      .withColumn("rejection_reason",concat_ws("; ",
        when(col("_user_found").isNull,"utilisateur absent ou rejeté"),
        when(col("_product_found").isNull,"produit absent ou rejeté"),
        when(col("_merchant_found").isNull,"marchand absent ou rejeté")))
    val rejected = joined.filter(col("rejection_reason") =!= "")
    val kept = joined.filter(col("rejection_reason") === "")
      .drop("rejection_reason","_user_found","_product_found","_merchant_found")
      .withColumn("amount",col("amount").cast("decimal(20,2)"))
      .withColumn("time_features",TimeFeatures.extractTimeFeatures(c)(col("timestamp")))
      .select(col("*"),col("time_features.*")).drop("time_features")
      .withColumn("transaction_ts",to_timestamp(col("timestamp"),"yyyyMMddHHmmss"))
      .withColumn("transaction_date",to_date(col("transaction_ts")))
      .withColumn("epoch_seconds",col("transaction_ts").cast("long"))
      .withColumn("age_group",when(col("age") < c.getInt("app.age.adult-start"),"Jeune")
        .when(col("age") < c.getInt("app.age.middle-start"),"Adulte")
        .when(col("age") < c.getInt("app.age.senior-start"),"Âge Moyen").otherwise("Senior"))
      .withColumn("catalog_merchant_mismatch",(col("merchant_id") =!= col("catalog_merchant_id")).cast("int"))
    Enrichment(addBehavior(kept),rejected)
  }
  def addBehavior(df: DataFrame): DataFrame = {
    val ordered = Window.partitionBy("user_id").orderBy(col("epoch_seconds"),col("transaction_id"))
    val user = Window.partitionBy("user_id")
    val range = Window.partitionBy("user_id").orderBy("epoch_seconds")
    val rolling = range.rangeBetween(-c.getLong("app.time.rolling-days")*86400L+1L,0L)
    val historical = range.rangeBetween(Window.unboundedPreceding,-1L)
    df.withColumn("transaction_rank",row_number().over(ordered))
      .withColumn("user_transaction_count",count(lit(1)).over(user))
      .withColumn("rolling_amount_7d",sum("amount").over(rolling))
      .withColumn("active_days_7d",size(collect_set(col("transaction_date")).over(rolling)))
      .withColumn("is_active",(col("active_days_7d") >= c.getInt("app.time.active-days")).cast("int"))
      .withColumn("previous_epoch",lag(col("epoch_seconds"),1).over(ordered))
      .withColumn("seconds_since_previous",col("epoch_seconds")-col("previous_epoch"))
      .withColumn("days_since_previous",col("seconds_since_previous")/86400.0)
      .withColumn("historical_average_amount",avg("amount").over(historical))
      .withColumn("excess_percent",when(col("historical_average_amount")>0,
        (col("amount")/col("historical_average_amount")-1)*100))
      .withColumn("suspicion_flags",
        coalesce((col("excess_percent")>c.getDouble("app.suspicion.excess-percent")).cast("int"),lit(0))+
        (col("day_period")==="Night").cast("int")+
        coalesce((col("seconds_since_previous")<c.getLong("app.suspicion.interval-seconds")).cast("int"),lit(0))+
        coalesce((col("payment_method")===c.getString("app.suspicion.payment-method")).cast("int"),lit(0)))
      .withColumn("is_suspicious",(col("suspicion_flags")>=c.getInt("app.suspicion.minimum-flags")).cast("int"))
      .drop("previous_epoch")
  }
}
