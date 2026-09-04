package com.ecommerce.models

// Option conserve les valeurs manquantes jusqu'à la validation.
case class Transaction(transaction_id: String, user_id: String, product_id: String,
  merchant_id: String, amount: Option[Double], timestamp: String, location: String,
  payment_method: String, category: String)
case class User(user_id: String, age: Option[Int], annual_income: Option[Double],
  city: String, customer_segment: String, preferred_categories: Seq[String], registration_date: String)
case class Product(product_id: String, name: String, category: String, price: Option[Double],
  merchant_id: String, rating: Option[Double], stock: Option[Int])
case class Merchant(merchant_id: String, name: String, category: String, region: String,
  commission_rate: Option[Double], establishment_date: String)
case class TimeInfo(hour: Int, day_of_week: String, month: String, is_weekend: Int,
  day_period: String, is_working_hours: Int)
case class QualityRow(dataset: String, nb_lignes_lues: Long, nb_lignes_valides: Long,
  nb_lignes_rejetees: Long, taux_rejet: Double, nb_valeurs_nulles: Long,
  nb_user_orphelins: Long, nb_product_orphelins: Long, nb_merchant_orphelins: Long)
case class Timing(mode: String, etape: String, duree_ms: Double)
