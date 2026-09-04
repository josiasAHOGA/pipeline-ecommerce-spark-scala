package com.ecommerce.analytics
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class Analytics(c: Config) {
  def merchants(df: DataFrame): DataFrame = {
    val groups = Seq("merchant_id","merchant_name","merchant_category","region")
    val base = df.groupBy(groups.map(col):_*).agg(
      sum("amount").as("revenue"),count(lit(1)).as("transactions"),countDistinct("user_id").as("unique_customers"),
      round(avg("amount"),2).as("average_basket"),
      round(sum(col("amount")*col("commission_rate").cast("decimal(12,6)")),2).as("commission"),
      round(avg("is_suspicious")*100,2).as("suspicious_rate"))
    base.withColumn("rank_category",dense_rank().over(Window.partitionBy("merchant_category").orderBy(col("revenue").desc)))
      .withColumn("rank_region",dense_rank().over(Window.partitionBy("region").orderBy(col("revenue").desc)))
      .orderBy(col("revenue").desc,col("merchant_id"))
  }
  def merchantAges(df: DataFrame): DataFrame = df.groupBy("merchant_id","age_group")
    .agg(sum("amount").as("revenue"),count(lit(1)).as("transactions"))
    .withColumn("revenue_share_percent",round(col("revenue")/sum("revenue").over(Window.partitionBy("merchant_id"))*100,2))
  def cohorts(df: DataFrame): DataFrame = {
    val monthly = df.withColumn("transaction_month",trunc(col("transaction_date"),"month"))
    val users = monthly.groupBy("user_id").agg(min("transaction_month").as("cohort_month"))
    val sizes = users.groupBy("cohort_month").agg(count(lit(1)).as("cohort_size"))
    val observed = monthly.agg(max("transaction_month").as("last_month"))
    // Génère uniquement les mois observables ; une cohorte récente n'est pas un échec à M+3.
    val grid = sizes.crossJoin(observed).withColumn("transaction_month",
      explode(sequence(col("cohort_month"),col("last_month"),expr("interval 1 month"))))
      .withColumn("period_index",months_between(col("transaction_month"),col("cohort_month")).cast("int"))
    val activity = monthly.join(users,Seq("user_id"))
      .groupBy("cohort_month","transaction_month").agg(countDistinct("user_id").as("active_users"),sum("amount").as("revenue"))
    grid.join(activity,Seq("cohort_month","transaction_month"),"left")
      .na.fill(0L,Seq("active_users")).na.fill(0.0,Seq("revenue"))
      .withColumn("retention_percent",round(col("active_users")/col("cohort_size")*100,2))
      .withColumn("revenue_per_initial_user",round(col("revenue")/col("cohort_size"),2))
      .withColumn("revenue_per_active_user",when(col("active_users")>0,round(col("revenue")/col("active_users"),2)))
      .drop("last_month").orderBy("cohort_month","period_index")
  }
  def rfm(df: DataFrame): DataFrame = {
    val reference = df.agg(max("transaction_date").as("reference_date"))
    val users = df.groupBy("user_id","customer_segment").agg(max("transaction_date").as("last_purchase"),
      count(lit(1)).as("frequency"),sum("amount").as("monetary"))
      .crossJoin(reference).withColumn("recency",datediff(col("reference_date"),col("last_purchase")))
    users.withColumn("r_score",ntile(5).over(Window.orderBy(col("recency").desc,col("user_id"))))
      .withColumn("f_score",ntile(5).over(Window.orderBy(col("frequency"),col("user_id"))))
      .withColumn("m_score",ntile(5).over(Window.orderBy(col("monetary"),col("user_id"))))
      .withColumn("rfm_segment",when(col("r_score")>=4 && col("f_score")>=4 && col("m_score")>=4,"Champions")
        .when(col("r_score")>=4 && col("f_score")<=2,"Nouveaux")
        .when(col("r_score")<=2 && col("f_score")>=3,"À risque")
        .when(col("r_score")<=2,"Perdus").otherwise("Clients fidèles"))
  }
  def all(df: DataFrame): Seq[(String,DataFrame)] = {
    val co = cohorts(df); val rf = rfm(df)
    Seq(
      "merchant_kpis" -> merchants(df), "merchant_age_sales" -> merchantAges(df),
      "cohort_retention" -> co,
      "cohort_matrix" -> co.groupBy("cohort_month","cohort_size").pivot("period_index").agg(first("retention_percent")).orderBy("cohort_month"),
      "best_cohort_m3" -> co.filter(col("period_index")===c.getInt("app.analytics.cohort-target-month"))
        .withColumn("retention_rank",dense_rank().over(Window.orderBy((col("active_users")/col("cohort_size")).desc)))
        .filter(col("retention_rank")===1),
      "rfm_customers" -> rf,
      "rfm_cross_segments" -> rf.groupBy("rfm_segment").pivot("customer_segment").count().na.fill(0L),
      "top_products" -> df.groupBy("product_id","product_name","stock").agg(sum("amount").as("revenue"),avg("rating").as("average_rating"))
        .orderBy(col("revenue").desc,col("product_id")).limit(c.getInt("app.analytics.top-products")),
      "category_region" -> df.groupBy("product_category","region").agg(sum("amount").as("revenue"),count(lit(1)).as("transactions"))
        .withColumn("region_share_percent",round(col("revenue")/sum("revenue").over(Window.partitionBy("region"))*100,2)),
      "payments_day_period" -> df.groupBy("payment_method","day_period").agg(sum("amount").as("revenue"),count(lit(1)).as("transactions")),
      "suspicious_top20" -> df.filter(col("is_suspicious")===1).orderBy(col("amount").desc,col("transaction_id"))
        .limit(c.getInt("app.analytics.top-suspicious")),
      "summary" -> df.agg(count(lit(1)).as("transactions"),sum("amount").as("revenue"),countDistinct("user_id").as("customers"),
        countDistinct("merchant_id").as("merchants"),sum("is_suspicious").as("suspicious_transactions"),
        sum("catalog_merchant_mismatch").as("catalog_merchant_mismatches"),min("transaction_date").as("first_date"),max("transaction_date").as("last_date"))
    )
  }
}
