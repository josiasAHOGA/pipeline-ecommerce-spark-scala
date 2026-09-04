package com.ecommerce

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.ecommerce.analytics._
import com.ecommerce.models._
import com.ecommerce.report.DashboardReport
import com.ecommerce.utils._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Suite de régression du Groupe 9, exécutée par `sbt test`.
 *
 * Choix assumé : la suite ne dépend d'aucun framework de test externe. Elle
 * s'exécute donc partout où le JAR s'exécute, y compris sur un poste hors
 * ligne ou dans l'image Docker, sans résolution de dépendances supplémentaire.
 *
 * Contrairement à une suite qui s'arrête au premier échec, chaque cas est
 * isolé : un test rouge n'empêche pas les suivants de s'exécuter. Un rapport
 * JUnit XML est écrit dans target/test-reports pour que l'intégration continue
 * affiche le détail cas par cas.
 */
object RegressionSuite {

  private case class Resultat(nom: String, reussi: Boolean, message: String, dureeMs: Long)
  private val resultats = ArrayBuffer.empty[Resultat]

  private def check(nom: String)(f: => Unit): Unit = {
    val debut = System.nanoTime()
    val issue =
      try { f; Resultat(nom, reussi = true, "", 0L) }
      catch {
        case e: AssertionError => Resultat(nom, reussi = false, Option(e.getMessage).getOrElse("assertion échouée"), 0L)
        case NonFatal(e) => Resultat(nom, reussi = false, e.getClass.getSimpleName + " : " + e.getMessage, 0L)
      }
    val duree = (System.nanoTime() - debut) / 1000000L
    resultats += issue.copy(dureeMs = duree)
    val etat = if (issue.reussi) "PASS" else "FAIL"
    println(f"$etat%-5s $duree%6d ms  $nom")
    if (!issue.reussi) println("        " + issue.message)
  }

  private def echapper(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  private def ecrireRapport(): Unit = {
    val echecs = resultats.count(!_.reussi)
    val duree = resultats.map(_.dureeMs).sum / 1000.0
    val cas = resultats.map { r =>
      val corps =
        if (r.reussi) ""
        else "<failure message=\"" + echapper(r.message) + "\">" + echapper(r.message) + "</failure>"
      "  <testcase classname=\"com.ecommerce.RegressionSuite\" name=\"" + echapper(r.nom) +
        "\" time=\"" + (r.dureeMs / 1000.0) + "\">" + corps + "</testcase>"
    }.mkString("\n")
    val xml =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<testsuite name=\"com.ecommerce.RegressionSuite\" tests=\"" + resultats.size +
        "\" failures=\"" + echecs + "\" errors=\"0\" time=\"" + duree + "\">\n" + cas + "\n</testsuite>\n"
    val dossier = Paths.get("target", "test-reports")
    Files.createDirectories(dossier)
    Files.write(dossier.resolve("regression.xml"), xml.getBytes(StandardCharsets.UTF_8))
    println("Rapport JUnit : target/test-reports/regression.xml")
  }

  def main(args: Array[String]): Unit = {
    val c = ConfigLoader.load()
    val spark = SparkSessionBuilder.build(c)
    import spark.implicits._
    try {

      // ------------------------------------------------ Partie 3 : temps

      check("parsing strict des dates, valeurs nulles et année bissextile") {
        val pattern = c.getString("app.validation.transaction.timestamp-pattern")
        assert(TimeFeatures.parse("20240229093000", pattern).isDefined, "2024 est bissextile")
        Seq(null, "", "20250229093000", "20241301000000", "2024010100000x")
          .foreach(s => assert(TimeFeatures.parse(s, pattern).isEmpty, "entrée invalide acceptée : " + s))
      }

      check("UDF horaires : nuit, frontières, week end et null") {
        val rows = Seq("20240106055959", "20240106060000", "20240106120000", "20240106175959",
          "20240106180000", "20240106220000", null)
          .toDF("timestamp").select(TimeFeatures.extractTimeFeatures(c)(col("timestamp")).as("t")).collect()
        val periods = rows.take(6).map(_.getStruct(0).getAs[String]("day_period")).toSeq
        assert(periods == Seq("Night", "Morning", "Afternoon", "Afternoon", "Evening", "Night"),
          "périodes obtenues : " + periods.mkString(", "))
        assert(rows(3).getStruct(0).getAs[Int]("is_working_hours") == 1, "17h59 doit rester ouvré")
        assert(rows(4).getStruct(0).getAs[Int]("is_working_hours") == 0, "18h n'est plus ouvré")
        assert(rows(0).getStruct(0).getAs[Int]("is_weekend") == 1, "le 6 janvier 2024 est un samedi")
        assert(rows.last.isNullAt(0), "un horodatage null doit produire null, pas une exception")
      }

      check("frontières exactes de day_period à 21h59, 22h00 et minuit") {
        val rows = Seq("20240103215959", "20240103220000", "20240103000000")
          .toDF("timestamp").select(TimeFeatures.extractTimeFeatures(c)(col("timestamp")).as("t"))
          .collect().map(_.getStruct(0).getAs[String]("day_period")).toSeq
        assert(rows == Seq("Evening", "Night", "Night"), "périodes obtenues : " + rows.mkString(", "))
      }

      // ------------------------------------------------ Partie 2 : validation

      val tx = Seq(
        Transaction("t1", "u", "p", "m", Some(10), "20240101090000", "Paris", "CARD", "Books"),
        Transaction("t2", "u", "p", "m", Some(-1), "20240101090000", "Paris", "CARD", "Books"),
        Transaction("t3", "u", "p", "m", None, null, "Paris", "CARD", "Books"),
        Transaction("t4", "u", "p", "m", Some(10), "20250229090000", "Paris", "CARD", "Books")
      ).toDS()
      val validator = new DataValidation(c)

      check("validation exhaustive : aucune ligne null ne disparaît") {
        val r = validator.transactions(tx.toDF)
        assert(r.valid.count() == 1, "une seule transaction est valide")
        assert(r.rejected.count() == 3, "trois transactions doivent être rejetées")
        assert(r.rejected.filter(col("transaction_id") === "t3").first()
          .getAs[String]("rejection_reason").contains(";"), "t3 viole deux règles, les deux motifs sont attendus")
      }

      check("bornes inclusives des référentiels") {
        val u = Seq(User("1", Some(16), Some(1), null, "Standard", Seq("Books"), "20240101"),
          User("2", Some(100), Some(1), null, "Standard", Seq.empty, "20240101"),
          User("3", Some(101), Some(1), null, "Standard", Seq.empty, "20240101"),
          User("4", None, None, null, "Standard", Seq.empty, "20240101")).toDF
        assert(validator.users(u).valid.count() == 2, "16 et 100 ans sont inclus, 101 et null exclus")
        val p = Seq(Product("1", "a", "Books", Some(1), "m", Some(1), Some(1)),
          Product("2", "b", "Books", Some(1), "m", Some(5), Some(1)),
          Product("3", "c", "Books", Some(0), "m", Some(6), Some(1))).toDF
        assert(validator.products(p).valid.count() == 2, "notes 1 et 5 incluses, prix nul exclu")
        val m = Seq(Merchant("1", "a", "Books", "Paris", Some(0), "20240101"),
          Merchant("2", "b", "Books", "Paris", Some(1), "20240101"),
          Merchant("3", "c", "Books", "Paris", Some(Double.NaN), "20240101")).toDF
        assert(validator.merchants(m).valid.count() == 2, "NaN doit être traité comme invalide")
      }

      check("rapport qualité : taux arrondi et comptage des valeurs nulles") {
        val u = Seq(User("1", Some(30), Some(1000), "Paris", "Standard", Seq("Books"), "20240101"),
          User("2", Some(200), None, null, "Standard", Seq.empty, "20240101")).toDS
        val p = Seq(Product("p", "a", "Books", Some(1), "m", Some(3), Some(1))).toDS
        val m = Seq(Merchant("m", "a", "Books", "Paris", Some(0.1), "20240101")).toDS
        val s = Sources(tx, u, p, m)
        val q = validator.quality(spark, s, validator.validateAll(s))
        val ligneUsers = q.filter(col("dataset") === "users").first()
        assert(ligneUsers.getAs[Long]("nb_lignes_rejetees") == 1, "un utilisateur sur deux est rejeté")
        assert(ligneUsers.getAs[Double]("taux_rejet") == 50.0, "le taux de rejet doit valoir 50 pour cent")
        assert(ligneUsers.getAs[Long]("nb_valeurs_nulles") >= 2, "annual_income et city nuls doivent être comptés")
        val ligneTx = q.filter(col("dataset") === "transactions").first()
        // Les quatre transactions pointent vers l'utilisateur u, absent du référentiel brut.
        assert(ligneTx.getAs[Long]("nb_user_orphelins") == 4, "toute référence absente doit être comptée comme orpheline")
      }

      // ------------------------------------------------ Partie 3 : fenêtrage

      val samples = Seq(
        ("a", "u", 10.0, "20240101000000", "Night", "CARD"),
        ("b", "u", 20.0, "20240102000000", "Night", "CARD"),
        ("c", "u", 30.0, "20240103000000", "Night", "CARD"),
        ("d", "u", 40.0, "20240104000000", "Night", "CARD"),
        ("e", "u", 50.0, "20240105000000", "Night", "CARD"),
        ("f", "u", 1000.0, "20240108000000", "Night", "CRYPTO"),
        ("g", "u", 10.0, "20240108000100", "Night", "CARD"),
        ("z", "other", 999.0, "20240108000000", "Night", "CARD")
      ).toDF("transaction_id", "user_id", "amount", "timestamp", "day_period", "payment_method")
        .withColumn("transaction_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
        .withColumn("epoch_seconds", col("transaction_ts").cast("long"))
        .withColumn("transaction_date", to_date(col("transaction_ts")))
      val behavior = new DataTransformation(c).addBehavior(samples).cache()

      check("fenêtre de sept jours, frontière exclue et utilisateurs isolés") {
        val f = behavior.filter(col("transaction_id") === "f").first()
        assert(math.abs(f.getAs[Double]("rolling_amount_7d") - 1140) < 0.001,
          "la transaction du 1er janvier sort de la fenêtre glissante")
        assert(f.getAs[Int]("active_days_7d") == 5 && f.getAs[Int]("is_active") == 1, "cinq jours distincts attendus")
        assert(behavior.filter(col("transaction_id") === "z").first()
          .getAs[Double]("rolling_amount_7d") == 999, "la fenêtre est partitionnée par utilisateur")
      }

      check("historique sans fuite future, premier achat et intervalle en secondes") {
        val f = behavior.filter(col("transaction_id") === "f").first()
        assert(f.getAs[Double]("historical_average_amount") == 30, "la moyenne exclut la transaction courante")
        assert(f.getAs[Int]("is_suspicious") == 1, "montant excessif, nuit et CRYPTO : trois signaux")
        assert(behavior.filter(col("transaction_id") === "g").first()
          .getAs[Long]("seconds_since_previous") == 60, "le délai est mesuré en secondes exactes")
        assert(behavior.filter(col("transaction_id") === "a").first()
          .getAs[Any]("days_since_previous") == null, "sans achat antérieur, le délai reste null")
      }

      check("un seul signal ne suffit pas à marquer une transaction suspecte") {
        val isole = Seq(("s1", "solo", 10.0, "20240101120000", "Afternoon", "CRYPTO"))
          .toDF("transaction_id", "user_id", "amount", "timestamp", "day_period", "payment_method")
          .withColumn("transaction_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
          .withColumn("epoch_seconds", col("transaction_ts").cast("long"))
          .withColumn("transaction_date", to_date(col("transaction_ts")))
        val r = new DataTransformation(c).addBehavior(isole).first()
        assert(r.getAs[Int]("suspicion_flags") == 1, "seul le moyen de paiement est en cause")
        assert(r.getAs[Int]("is_suspicious") == 0, "le seuil du sujet est de deux conditions simultanées")
      }

      check("compte de jours distincts et non nombre de transactions") {
        val repeated = samples.filter(col("transaction_id") === "a").union(samples.filter(col("transaction_id") === "a"))
        assert(new DataTransformation(c).addBehavior(repeated).first().getAs[Int]("active_days_7d") == 1,
          "deux achats le même jour ne font pas deux jours actifs")
      }

      // ------------------------------------------------ Partie 4 : analytique

      check("cohortes : mois absent égal zéro, futur non observable exclu") {
        val df = Seq(("u1", "2024-01-01", 10.0), ("u1", "2024-03-01", 20.0),
          ("u2", "2024-01-02", 30.0), ("u3", "2024-03-02", 40.0))
          .toDF("user_id", "date", "amount").withColumn("transaction_date", to_date(col("date")))
        val co = new Analytics(c).cohorts(df)
        assert(co.count() == 4, "deux cohortes, trois mois observables au total")
        assert(co.filter(col("cohort_month") === "2024-01-01" && col("period_index") === 1)
          .first().getAs[Long]("active_users") == 0, "un mois observable sans activité vaut zéro")
        assert(co.filter(col("cohort_month") === "2024-01-01" && col("period_index") === 2)
          .first().getAs[Double]("retention_percent") == 50, "un client sur deux revient à M+2")
        assert(co.filter(col("period_index") === 0 && col("retention_percent") =!= 100).count() == 0,
          "M+0 vaut toujours cent pour cent par construction")
      }

      check("scores RFM : la récence la plus fraîche obtient le meilleur score") {
        val df = Seq(
          ("recent", "Premium", "2025-12-30", 100.0), ("recent", "Premium", "2025-12-31", 100.0),
          ("ancien", "Standard", "2024-01-01", 10.0))
          .toDF("user_id", "customer_segment", "date", "amount")
          .withColumn("transaction_date", to_date(col("date")))
        val r = new Analytics(c).rfm(df).collect().map(x => x.getAs[String]("user_id") -> x).toMap
        assert(r("recent").getAs[Int]("r_score") > r("ancien").getAs[Int]("r_score"),
          "un score de récence élevé doit signaler un achat récent")
        assert(r("recent").getAs[Long]("frequency") == 2, "la fréquence compte les transactions")
        assert(r("ancien").getAs[Int]("recency") > 300, "la récence se mesure en jours depuis la date de référence")
      }

      check("KPI marchands : classement dense par catégorie et par région") {
        val df = Seq(
          ("m1", "Alpha", "Books", "IDF", "u1", 100.0, 0.10, 0),
          ("m2", "Beta", "Books", "IDF", "u2", 50.0, 0.20, 1),
          ("m3", "Gamma", "Books", "Sud", "u3", 300.0, 0.05, 0))
          .toDF("merchant_id", "merchant_name", "merchant_category", "region", "user_id",
            "amount", "commission_rate", "is_suspicious")
        val kpi = new Analytics(c).merchants(df).collect().map(x => x.getAs[String]("merchant_id") -> x).toMap
        assert(kpi("m3").getAs[Int]("rank_category") == 1, "Gamma domine la catégorie Books")
        assert(kpi("m1").getAs[Int]("rank_region") == 1, "Alpha domine la région IDF")
        assert(kpi("m3").getAs[Int]("rank_region") == 1, "chaque région a son propre classement")
        // Selon le type d'entrée, Spark renvoie un Double ou un BigDecimal : on lit les deux.
        val commission = kpi("m2").getAs[Any]("commission") match {
          case d: java.math.BigDecimal => d.doubleValue
          case n: Number => n.doubleValue
          case autre => throw new AssertionError("type de commission inattendu : " + autre)
        }
        assert(math.abs(commission - 10.0) < 0.001,
          "la commission vaut le montant multiplié par le taux du marchand")
      }

      // ------------------------------------------------ Jointures et bout en bout

      check("jointures expliquées, aucun orphelin silencieux, âge 25 couvert") {
        val u = Seq(User("u", Some(25), Some(1000), "Paris", "Standard", Seq("Books"), "20230101")).toDS
        val p = Seq(Product("p", "Livre", "Books", Some(10), "m", Some(5), Some(1))).toDS
        val m = Seq(Merchant("m", "Boutique", "Books", "IDF", Some(0.1), "20200101")).toDS
        val t = tx.filter(col("transaction_id") === "t1")
          .union(Seq(Transaction("orphan", "absent", "p", "m", Some(5), "20240102090000", "Paris", "CARD", "Books")).toDS)
        val s = Sources(t, u, p, m); val v = validator.validateAll(s)
        val e = new DataTransformation(c).enrichTransactionData(v, true)
        assert(e.data.count() == 1 && e.rejected.count() == 1, "la transaction orpheline est isolée, pas perdue")
        assert(e.data.first().getAs[String]("age_group") == "Adulte", "25 ans appartient à la tranche Adulte")
        assert(validator.orphanCount(t.toDF, u.toDF, "user_id") == 1, "un utilisateur référencé est absent")
        val kpi = new Analytics(c).merchants(e.data).first()
        assert(kpi.getAs[java.math.BigDecimal]("revenue").doubleValue() == 10, "chiffre d'affaires attendu")
        assert(kpi.getAs[java.math.BigDecimal]("commission").doubleValue() == 1, "commission de dix pour cent")
      }

      // ------------------------------------------------ Partie 7 et restitution

      check("valeurs par défaut de configuration") {
        val partial = com.typesafe.config.ConfigFactory.parseString("app.spark.shuffle.partitions=2")
          .withFallback(com.typesafe.config.ConfigFactory.load()).resolve()
        assert(partial.getInt("app.spark.shuffle.partitions") == 2, "la surcharge doit primer")
        assert(partial.getInt("app.time.rolling-days") == 7, "une clé absente retombe sur reference.conf")
      }

      check("tableau de bord : page autonome produite depuis les sorties Gold") {
        val dossier = Files.createTempDirectory("dashboard-test")
        val resume = Seq((124751L, 49897506.62, 10193L, 586L, 3760L, 512L, "2024-01-01", "2025-12-31"))
          .toDF("transactions", "revenue", "customers", "merchants",
            "suspicious_transactions", "catalog_merchant_mismatches", "first_date", "last_date")
        val qualite = Seq(QualityRow("transactions", 138047L, 136157L, 1890L, 1.37, 940L, 400L, 300L, 250L)).toDF
        val cohortes = Seq(("2024-01-01", 0, 100.0, 120L, 120L), ("2024-01-01", 3, 42.5, 120L, 51L))
          .toDF("cohort_month", "period_index", "retention_percent", "cohort_size", "active_users")
        new DashboardReport(c).render(spark, dossier.toString,
          Seq("summary" -> resume, "quality_report" -> qualite, "cohort_retention" -> cohortes),
          Seq(Timing("avec", "total", 148000.0)))
        val page = dossier.resolve("dashboard.html")
        assert(Files.exists(page), "le pipeline doit écrire dashboard.html dans le répertoire de sortie")
        val html = new String(Files.readAllBytes(page), StandardCharsets.UTF_8)
        assert(html.length > 4000, "la page produite est anormalement courte : " + html.length)
        assert(html.contains("Transactions analysées"), "les indicateurs de tête sont absents")
        assert(html.contains("data-theme"), "le commutateur de thème clair et sombre doit être présent")
        assert(html.contains("localStorage"), "le thème choisi doit être mémorisé dans le navigateur")
        assert(!html.contains("http://") && !html.contains("https://"),
          "la page doit rester autonome, sans aucune dépendance réseau")
      }

      behavior.unpersist()

    } finally {
      spark.stop()
      ecrireRapport()
      val echecs = resultats.count(!_.reussi)
      val reussis = resultats.size - echecs
      println("")
      println("=" * 64)
      println(s"RÉSULTAT : $reussis réussis, $echecs échecs, ${resultats.size} cas au total")
      println("=" * 64)
      if (echecs > 0) {
        resultats.filterNot(_.reussi).foreach(r => println("ÉCHEC : " + r.nom + " -> " + r.message))
        sys.exit(1)
      }
    }
  }
}
