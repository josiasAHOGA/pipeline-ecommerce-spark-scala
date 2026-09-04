package com.ecommerce.analytics

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

/**
 * Résultat de l'enrichissement : les transactions exploitables d'un côté, les
 * lignes écartées à la jointure de l'autre, avec leur motif. Aucune ligne n'est
 * perdue en silence, exigence reprise de la Question 2.2 et appliquée ici aux
 * rejets de jointure.
 */
case class Enrichment(data: DataFrame, rejected: DataFrame)

/**
 * Partie 3 du sujet : transformations avancées.
 *
 * Deux responsabilités, volontairement séparées en deux méthodes publiques.
 *
 *  - `enrichTransactionData` (Question 3.2) joint les quatre tables, applique
 *    l'UDF temporelle et calcule la tranche d'âge.
 *  - `addBehavior` (Questions 3.3 et 3.4) ajoute les colonnes issues de
 *    fonctions de fenêtrage : rang, compteur, cumul glissant, jours actifs,
 *    délai entre achats, moyenne historique et détection de suspicion.
 *
 * La séparation n'est pas cosmétique : `addBehavior` prend un DataFrame déjà
 * aplati et se teste donc sur sept lignes construites à la main, sans avoir à
 * fabriquer les quatre référentiels.
 */
class DataTransformation(c: Config) {

  import DataTransformation.SecondesParJour

  /**
   * Marque une table de référence comme diffusable (Question 5.2).
   *
   * `spark.sql.autoBroadcastJoinThreshold` vaut -1 dans la configuration : la
   * diffusion automatique est désactivée pour que les jointures soient un choix
   * explicite et mesurable. Sans ce `broadcast`, chaque jointure déclencherait
   * un tri et un échange réseau des 138 000 transactions. Les trois
   * référentiels tiennent largement en mémoire d'exécuteur : 600 marchands,
   * 6 000 produits, 12 000 utilisateurs.
   *
   * Le drapeau `optimized` permet au mode `benchmark` de rejouer le pipeline
   * sans aucune optimisation, pour chiffrer le gain.
   */
  private def diffusable(df: DataFrame, optimized: Boolean): DataFrame =
    if (optimized && c.getBoolean("app.optimization.enable-broadcast")) broadcast(df) else df

  /**
   * Question 3.2 : jointure des quatre tables, caractéristiques temporelles et
   * tranche d'âge.
   *
   * Stratégie de jointure retenue, table par table :
   *
   * | Table        | Type   | Clé         | Raison |
   * | transactions | départ | -           | table de faits, elle impose la granularité : une ligne de sortie par transaction |
   * | users        | left   | user_id     | garder la transaction pour pouvoir expliquer une référence absente au lieu de la faire disparaître |
   * | products     | left   | product_id  | idem |
   * | merchants    | left   | merchant_id | idem, plus diffusion de la table |
   *
   * Une jointure `inner` donnerait exactement le même périmètre final, puisque
   * les lignes non appariées sont ensuite écartées. La différence est la
   * traçabilité : ici elles partent dans `join_rejections` avec leur motif,
   * alors qu'un `inner` les supprimerait sans laisser de trace. C'est le même
   * principe que la validation de la Partie 2.
   *
   * Les clés sont passées via `Seq("user_id")` et non `t("user_id") === u(...)`
   * pour que Spark ne conserve qu'un seul exemplaire de la colonne de jointure.
   * Les colonnes homonymes (`name`, `category`, `merchant_id` du catalogue) sont
   * renommées avant la jointure, sinon la sélection devient ambiguë.
   */
  def enrichTransactionData(v: Map[String, ValidationResult], optimized: Boolean): Enrichment = {
    // Un drapeau constant posé avant la jointure : resté nul après un left
    // join, il prouve que la référence n'a pas été appariée. Tester une colonne
    // métier ne suffirait pas, car elle peut être nulle dans la source.
    val users = v("users").valid.withColumn("_user_found", lit(true))

    val products = v("products").valid
      .select(
        col("product_id"),
        col("name").as("product_name"),
        col("category").as("product_category"),
        col("price"),
        col("rating"),
        col("stock"),
        col("merchant_id").as("catalog_merchant_id"))
      .withColumn("_product_found", lit(true))

    val merchants = v("merchants").valid
      .select(
        col("merchant_id"),
        col("name").as("merchant_name"),
        col("category").as("merchant_category"),
        col("region"),
        col("commission_rate"))
      .withColumn("_merchant_found", lit(true))

    val drapeaux = Seq("_user_found", "_product_found", "_merchant_found")

    val joined = v("transactions").valid
      .join(diffusable(users, optimized), Seq("user_id"), "left")
      .join(diffusable(products, optimized), Seq("product_id"), "left")
      .join(diffusable(merchants, optimized), Seq("merchant_id"), "left")
      // concat_ws ignore les when non satisfaits, qui valent null : la chaîne
      // reste vide si aucune règle n'est violée, et cumule les motifs sinon.
      .withColumn("rejection_reason", concat_ws("; ",
        when(col("_user_found").isNull, "utilisateur absent ou rejeté"),
        when(col("_product_found").isNull, "produit absent ou rejeté"),
        when(col("_merchant_found").isNull, "marchand absent ou rejeté")))

    val rejected = joined.filter(col("rejection_reason") =!= "").drop(drapeaux: _*)

    val kept = joined.filter(col("rejection_reason") === "")
      .drop("rejection_reason").drop(drapeaux: _*)
      // decimal évite la dérive des flottants sur des sommes de montants : un
      // chiffre d'affaires doit être reproductible au centime.
      .withColumn("amount", col("amount").cast("decimal(20,2)"))
      // La structure renvoyée par l'UDF est aplatie en six colonnes.
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(c)(col("timestamp")))
      .select(col("*"), col("time_features.*")).drop("time_features")
      // Colonnes techniques nécessaires au fenêtrage : un horodatage typé, la
      // date seule pour compter des jours distincts, et les secondes depuis
      // l'epoch, seule échelle acceptée par rangeBetween.
      .withColumn("transaction_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("transaction_date", to_date(col("transaction_ts")))
      .withColumn("epoch_seconds", col("transaction_ts").cast("long"))
      // Tranche d'âge : le sujet écrit « moins de 25 » puis « 26 à 44 » et
      // laisse 25 ans sans classe. Nous rattachons 25 ans à Adulte, convention
      // annoncée dans CONTRIBUTIONS.md et couverte par un test.
      .withColumn("age_group",
        when(col("age") < c.getInt("app.age.adult-start"), "Jeune")
          .when(col("age") < c.getInt("app.age.middle-start"), "Adulte")
          .when(col("age") < c.getInt("app.age.senior-start"), "Âge Moyen")
          .otherwise("Senior"))
      // Contrôle de cohérence : le marchand de la transaction n'est pas
      // toujours celui qui vend le produit dans le catalogue.
      .withColumn("catalog_merchant_mismatch",
        (col("merchant_id") =!= col("catalog_merchant_id")).cast("int"))

    Enrichment(addBehavior(kept), rejected)
  }

  /**
   * Questions 3.3 et 3.4 : analyse par partition Window.
   *
   * Entrée attendue : un DataFrame contenant au minimum `user_id`,
   * `transaction_id`, `amount`, `epoch_seconds`, `transaction_date`,
   * `day_period` et `payment_method`. Le contrat est vérifié à l'entrée, sinon
   * un nom de colonne absent ne se manifesterait qu'au premier `count`, loin de
   * la cause.
   *
   * Quatre fenêtres cohabitent, et c'est le point à comprendre de ce module :
   *
   *  - `ordered` : classement par utilisateur. `orderBy(epoch_seconds,
   *    transaction_id)` départage les ex æquo, sans quoi `row_number` et `lag`
   *    varieraient d'une exécution à l'autre.
   *  - `parUtilisateur` : toute l'histoire de l'utilisateur, sans ordre, pour
   *    un simple total.
   *  - `glissante` : `rangeBetween` sur des secondes. Une fenêtre en `rowsBetween`
   *    compterait des lignes, pas du temps : sept transactions ne sont pas sept
   *    jours. La borne basse est `-(7 * 86400) + 1`, donc les sept dernières
   *    fois vingt quatre heures, borne inférieure exclue.
   *  - `historique` : de la première transaction jusqu'à l'instant précédant la
   *    transaction courante (`unboundedPreceding` à `-1` seconde). Cela exclut
   *    la transaction courante de sa propre moyenne, et avec elle toute
   *    transaction du même instant : une moyenne qui contiendrait la ligne
   *    évaluée ne pourrait jamais la déclarer anormale.
   */
  def addBehavior(df: DataFrame): DataFrame = {
    val requises = Seq("user_id", "transaction_id", "amount", "epoch_seconds",
      "transaction_date", "day_period", "payment_method")
    val absentes = requises.filterNot(df.columns.contains)
    require(absentes.isEmpty, s"addBehavior : colonnes absentes ${absentes.mkString(", ")}")

    val joursGlissants = c.getLong("app.time.rolling-days")
    val ordered: WindowSpec = Window.partitionBy("user_id").orderBy(col("epoch_seconds"), col("transaction_id"))
    val parUtilisateur: WindowSpec = Window.partitionBy("user_id")
    val temporelle = Window.partitionBy("user_id").orderBy(col("epoch_seconds"))
    val glissante = temporelle.rangeBetween(-joursGlissants * SecondesParJour + 1L, 0L)
    val historique = temporelle.rangeBetween(Window.unboundedPreceding, -1L)

    df
      // Question 3.2 : rang de la transaction et total par utilisateur.
      .withColumn("transaction_rank", row_number().over(ordered))
      .withColumn("user_transaction_count", count(lit(1)).over(parUtilisateur))
      // Question 3.3 : montant cumulé sur sept jours glissants.
      .withColumn("rolling_amount_7d", sum("amount").over(glissante))
      // Question 3.3 : utilisateur actif. collect_set dédoublonne les dates,
      // donc deux achats le même jour comptent pour un seul jour actif ; un
      // count aurait compté des transactions.
      .withColumn("active_days_7d", size(collect_set(col("transaction_date")).over(glissante)))
      .withColumn("is_active", (col("active_days_7d") >= c.getInt("app.time.active-days")).cast("int"))
      // Question 3.3 : délai depuis l'achat précédent, via lag.
      .withColumn("previous_epoch", lag(col("epoch_seconds"), 1).over(ordered))
      .withColumn("previous_date", lag(col("transaction_date"), 1).over(ordered))
      // Deux échelles assumées : datediff donne le nombre de jours entiers
      // demandé par le sujet, les secondes servent à la règle des cinq minutes
      // de la Question 3.4, qu'un arrondi au jour rendrait inapplicable.
      .withColumn("days_since_previous", datediff(col("transaction_date"), col("previous_date")))
      .withColumn("seconds_since_previous", col("epoch_seconds") - col("previous_epoch"))
      .withColumn("hours_since_previous", round(col("seconds_since_previous") / 3600.0, 2))
      // Question 3.4 : écart au panier moyen historique de l'utilisateur.
      .withColumn("historical_average_amount", avg("amount").over(historique))
      .withColumn("excess_percent",
        when(col("historical_average_amount") > 0,
          round((col("amount") / col("historical_average_amount") - 1) * 100, 2)))
      // Question 3.4 : chaque condition vaut un point. Le coalesce est
      // indispensable : sur une première transaction, l'historique est nul,
      // donc l'écart est nul, et null + 1 vaut null en SQL. Sans lui, un seul
      // champ manquant rendrait is_suspicious nul au lieu de 0.
      .withColumn("suspicion_flags",
        drapeau(col("excess_percent") > c.getDouble("app.suspicion.excess-percent")) +
          drapeau(col("day_period") === "Night") +
          drapeau(col("seconds_since_previous") < c.getLong("app.suspicion.interval-seconds")) +
          drapeau(col("payment_method") === c.getString("app.suspicion.payment-method")))
      .withColumn("is_suspicious",
        (col("suspicion_flags") >= c.getInt("app.suspicion.minimum-flags")).cast("int"))
      .drop("previous_epoch", "previous_date")
  }

  /** Convertit un prédicat éventuellement nul en 0 ou 1, jamais en null. */
  private def drapeau(condition: org.apache.spark.sql.Column) =
    coalesce(condition.cast("int"), lit(0))
}

object DataTransformation {
  /** Une journée en secondes, unité de `rangeBetween` sur `epoch_seconds`. */
  val SecondesParJour: Long = 24L * 60L * 60L
}
