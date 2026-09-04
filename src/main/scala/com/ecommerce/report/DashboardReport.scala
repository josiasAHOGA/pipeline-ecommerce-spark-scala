package com.ecommerce.report

import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.ecommerce.models.Timing
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Restitution décisionnelle du pipeline.
 *
 * Le programme écrit une application de tableau de bord dans un fichier HTML
 * unique, à la racine du répertoire de sortie. Elle ne dépend d'aucun réseau,
 * d'aucun serveur et d'aucune bibliothèque externe : elle s'ouvre par double
 * clic et reste utilisable hors ligne.
 *
 * Deux thèmes sont fournis, clair et sombre, avec un commutateur mémorisé dans
 * le navigateur. Toutes les valeurs affichées proviennent des DataFrame Gold
 * réellement produits par l'exécution en cours.
 */
class DashboardReport(c: Config) {

  private val decimal = java.text.NumberFormat.getNumberInstance(Locale.FRANCE)
  decimal.setMaximumFractionDigits(0)
  private val precise = java.text.NumberFormat.getNumberInstance(Locale.FRANCE)
  precise.setMinimumFractionDigits(2); precise.setMaximumFractionDigits(2)

  private val couleurs = Seq("var(--bleu)", "var(--violet)", "var(--ambre)", "var(--vert)", "var(--rose)", "var(--cyan)")

  // ---------------------------------------------------------------- lecture

  private def str(r: Row, name: String): String =
    if (r.schema.fieldNames.contains(name)) Option(r.getAs[Any](name)).map(_.toString).getOrElse("") else ""

  private def num(r: Row, name: String): Double =
    if (!r.schema.fieldNames.contains(name)) 0.0
    else Option(r.getAs[Any](name)).map {
      case d: java.math.BigDecimal => d.doubleValue
      case n: Number => n.doubleValue
      case s: String => Try(s.toDouble).getOrElse(0.0)
      case _ => 0.0
    }.getOrElse(0.0)

  private def escape(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  /**
   * Nombre destiné à une valeur CSS ou SVG.
   *
   * Le formatage d'affichage utilise la locale française et donc la virgule
   * décimale. Une virgule dans une largeur CSS ou un rayon SVG rend la valeur
   * invalide et l'élément disparaît : ces valeurs techniques sont donc
   * formatées en locale neutre.
   */
  private def technique(d: Double, decimales: Int): String =
    String.format(Locale.ROOT, "%." + decimales + "f", Double.box(d))

  private def entier(d: Double): String = decimal.format(d)
  private def euros(d: Double): String = precise.format(d) + " EUR"
  private def compact(d: Double): String =
    if (math.abs(d) >= 1000000) precise.format(d / 1000000) + " M" else decimal.format(d)
  private def pourcent(d: Double): String = precise.format(d) + " %"

  // ---------------------------------------------------------------- fragments

  private def kpi(valeur: String, libelle: String, marqueur: String, couleur: String): String =
    s"""<article class="card kpi">
       |<p class="kpi-label">${escape(libelle)}</p>
       |<p class="kpi-value">${escape(valeur)}</p>
       |<p class="chip" style="--c:$couleur">${escape(marqueur)}</p>
       |</article>""".stripMargin

  private def carte(id: String, titre: String, badge: String, corps: String, classe: String): String = {
    val marque = if (badge.isEmpty) "" else s"""<span class="badge">${escape(badge)}</span>"""
    s"""<section class="card $classe"${if (id.isEmpty) "" else s""" id="$id""""}>
       |<header class="card-head"><h2>${escape(titre)}</h2>$marque</header>
       |$corps
       |</section>""".stripMargin
  }

  private def table(entetes: Seq[String], lignes: Seq[Seq[String]], alignFin: Set[Int]): String = {
    val th = entetes.zipWithIndex.map { case (h, i) =>
      s"""<th${if (alignFin(i)) " class='num'" else ""}>${escape(h)}</th>""" }.mkString
    val tr = lignes.map { l =>
      val td = l.zipWithIndex.map { case (v, i) =>
        s"""<td${if (alignFin(i)) " class='num'" else ""}>${escape(v)}</td>""" }.mkString
      s"<tr>$td</tr>"
    }.mkString("\n")
    s"""<div class="scroll"><table><thead><tr>$th</tr></thead><tbody>
       |$tr
       |</tbody></table></div>""".stripMargin
  }

  /** Barres horizontales en CSS pur : aucune bibliothèque de graphiques. */
  private def barres(donnees: Seq[(String, Double)], format: Double => String, teinte: Boolean): String = {
    if (donnees.isEmpty) return ""
    val maxi = math.max(donnees.map(_._2).max, 1.0)
    val lignes = donnees.zipWithIndex.map { case ((nom, v), i) =>
      val largeur = technique(math.max(v / maxi * 100.0, 1.0), 2)
      val couleur = if (teinte) couleurs(i % couleurs.size) else "var(--bleu)"
      s"""<li><span class="bar-name">${escape(nom)}</span>
         |<span class="bar-track"><span class="bar-fill" style="width:$largeur%;background:$couleur"></span></span>
         |<span class="bar-value">${escape(format(v))}</span></li>""".stripMargin
    }.mkString("\n")
    s"""<ul class="bars">
       |$lignes
       |</ul>""".stripMargin
  }

  /** Anneau de répartition, dessiné en SVG sans dépendance externe. */
  private def anneau(donnees: Seq[(String, Double)], centreHaut: String, centreBas: String): String = {
    if (donnees.isEmpty) return ""
    val total = math.max(donnees.map(_._2).sum, 0.0001)
    val rayon = 54.0
    val circonference = 2 * math.Pi * rayon
    var offset = 0.0
    val arcs = donnees.zipWithIndex.map { case ((_, v), i) =>
      val longueur = v / total * circonference
      val arc =
        s"""<circle class="arc" cx="70" cy="70" r="${technique(rayon, 1)}" fill="none"
           | stroke="${couleurs(i % couleurs.size)}" stroke-width="18"
           | stroke-dasharray="${technique(longueur, 2)} ${technique(circonference - longueur, 2)}"
           | stroke-dashoffset="${technique(-offset, 2)}" />""".stripMargin.replace("\n", " ")
      offset += longueur
      arc
    }.mkString
    val legende = donnees.zipWithIndex.map { case ((nom, v), i) =>
      s"""<li><span class="puce" style="background:${couleurs(i % couleurs.size)}"></span>
         |<span class="leg-nom">${escape(nom)}</span>
         |<span class="leg-val">${precise.format(v / total * 100.0)}&nbsp;%</span></li>""".stripMargin
    }.mkString("\n")
    s"""<div class="anneau">
       |<div class="anneau-svg">
       |<svg viewBox="0 0 140 140" width="140" height="140" role="img" aria-label="Répartition">
       |<circle cx="70" cy="70" r="$rayon" fill="none" stroke="var(--piste)" stroke-width="18"/>
       |$arcs
       |</svg>
       |<div class="anneau-centre"><strong>${escape(centreHaut)}</strong><span>${escape(centreBas)}</span></div>
       |</div>
       |<ul class="legende">
       |$legende
       |</ul>
       |</div>""".stripMargin
  }

  /** Matrice de rétention : l'intensité de la couleur suit le taux observé. */
  private def heatmap(lignes: Seq[Row], colonnes: Int): String = {
    if (lignes.isEmpty) return """<p class="vide">Aucune cohorte observée.</p>"""
    val parCohorte = lignes.groupBy(r => str(r, "cohort_month")).toSeq.sortBy(_._1)
    val periodes = 0 to colonnes
    val entete = periodes.map(p => s"<th class='num'>M+$p</th>").mkString
    val corps = parCohorte.map { case (mois, rows) =>
      val taille = rows.headOption.map(r => num(r, "cohort_size")).getOrElse(0.0)
      val parPeriode = rows.map(r => num(r, "period_index").toInt -> r).toMap
      val cells = periodes.map { p =>
        parPeriode.get(p) match {
          case None => """<td class="cell neutre"></td>"""
          case Some(r) =>
            val taux = num(r, "retention_percent")
            val intensite = math.min(taux / 100.0, 1.0)
            val alpha = technique(0.12 + intensite * 0.85, 3)
            val fonce = if (intensite > 0.5) " fonce" else ""
            s"""<td class="cell$fonce" style="background:rgba(59,130,246,$alpha)">${precise.format(taux)}</td>"""
        }
      }.mkString
      s"""<tr><th class="cohorte">${escape(mois)}</th><td class="num taille">${entier(taille)}</td>$cells</tr>"""
    }.mkString("\n")
    s"""<div class="scroll"><table class="heat"><thead><tr>
       |<th>Cohorte</th><th class="num">Effectif</th>$entete
       |</tr></thead><tbody>
       |$corps
       |</tbody></table></div>""".stripMargin
  }

  // ---------------------------------------------------------------- rendu

  def render(spark: SparkSession, out: String, rapports: Seq[(String, DataFrame)], timings: Seq[Timing]): Unit = {
    try {
      val index = rapports.toMap
      def prendre(nom: String, n: Int): Seq[Row] =
        index.get(nom).map(df => df.limit(n).collect().toSeq).getOrElse(Seq.empty)

      val qualite = prendre("quality_report", 10)
      val resume = prendre("summary", 1).headOption
      val marchands = prendre("merchant_kpis", c.getInt("app.report.top-merchants"))
      val cohortes = prendre("cohort_retention", 2000)
      val meilleure = prendre("best_cohort_m3", 5)
      val rfm = prendre("rfm_cross_segments", 20)
      val produits = prendre("top_products", 10)
      val paiements = prendre("payments_day_period", 60)
      val categories = prendre("category_region", 500)
      val colonnes = c.getInt("app.report.retention-columns")
      val horizon = c.getInt("app.analytics.cohort-target-month")

      val transactions = resume.map(r => num(r, "transactions")).getOrElse(0.0)
      val revenu = resume.map(r => num(r, "revenue")).getOrElse(0.0)
      val clients = resume.map(r => num(r, "customers")).getOrElse(0.0)
      val enseignes = resume.map(r => num(r, "merchants")).getOrElse(0.0)
      val suspectes = resume.map(r => num(r, "suspicious_transactions")).getOrElse(0.0)
      val debut = resume.map(r => str(r, "first_date")).getOrElse("")
      val fin = resume.map(r => str(r, "last_date")).getOrElse("")
      val panier = if (transactions > 0) revenu / transactions else 0.0
      val tauxSuspect = if (transactions > 0) suspectes / transactions * 100.0 else 0.0

      val ligneTx = qualite.find(r => str(r, "dataset") == "transactions")
      val luesTx = ligneTx.map(r => num(r, "nb_lignes_lues")).getOrElse(0.0)
      val validesTx = ligneTx.map(r => num(r, "nb_lignes_valides")).getOrElse(0.0)

      val horodatage = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH'h'mm"))
      val total = timings.find(_.etape == "total").map(_.duree_ms / 1000.0).getOrElse(0.0)

      // ---- indicateurs de tête
      val enTete =
        s"""<div class="grid kpis">
           |${kpi(entier(transactions), "Transactions analysées", debut + " au " + fin, "var(--bleu)")}
           |${kpi(compact(revenu) + " EUR", "Chiffre d'affaires", "Montants retenus", "var(--violet)")}
           |${kpi(entier(clients), "Clients uniques", "Acheteurs distincts", "var(--vert)")}
           |${kpi(entier(enseignes), "Marchands actifs", "Au moins une vente", "var(--cyan)")}
           |${kpi(euros(panier), "Panier moyen", "CA / transactions", "var(--ambre)")}
           |${kpi(pourcent(tauxSuspect), "Signalées", entier(suspectes) + " lignes", "var(--rose)")}
           |</div>""".stripMargin

      // ---- qualité
      val entonnoir = {
        val etapes = Seq(("Lues", luesTx), ("Valides", validesTx), ("Analysées", transactions))
        val maxi = math.max(etapes.map(_._2).max, 1.0)
        val li = etapes.zipWithIndex.map { case ((nom, v), i) =>
          val largeur = technique(v / maxi * 100.0, 2)
          s"""<li><span class="fun-name">${escape(nom)}</span>
             |<span class="fun-track"><span class="fun-fill" style="width:$largeur%;background:${couleurs(i)}">${entier(v)}</span></span>
             |<span class="fun-value">${precise.format(v / maxi * 100.0)}&nbsp;%</span></li>""".stripMargin
        }.mkString("\n")
        s"""<ul class="funnel">$li</ul>"""
      }
      val tableQualite = table(
        Seq("Jeu de données", "Lues", "Valides", "Rejetées", "Taux", "Nulls"),
        qualite.map(r => Seq(str(r, "dataset"), entier(num(r, "nb_lignes_lues")), entier(num(r, "nb_lignes_valides")),
          entier(num(r, "nb_lignes_rejetees")), pourcent(num(r, "taux_rejet")), entier(num(r, "nb_valeurs_nulles")))),
        Set(1, 2, 3, 4, 5))
      val orphelins = ligneTx.map { r =>
        s"""<div class="mini-stats">
           |<div><strong>${entier(num(r, "nb_user_orphelins"))}</strong><span>user_id orphelins</span></div>
           |<div><strong>${entier(num(r, "nb_product_orphelins"))}</strong><span>product_id orphelins</span></div>
           |<div><strong>${entier(num(r, "nb_merchant_orphelins"))}</strong><span>merchant_id orphelins</span></div>
           |</div>""".stripMargin
      }.getOrElse("")

      // ---- marchands
      val tableMarchands = table(
        Seq("Marchand", "Catégorie", "Région", "Chiffre d'affaires", "Transactions", "Clients", "Panier moyen", "Commission", "Rang cat."),
        marchands.map(r => Seq(str(r, "merchant_name"), str(r, "merchant_category"), str(r, "region"),
          euros(num(r, "revenue")), entier(num(r, "transactions")), entier(num(r, "unique_customers")),
          euros(num(r, "average_basket")), euros(num(r, "commission")), entier(num(r, "rank_category")))),
        Set(3, 4, 5, 6, 7, 8))
      val barresMarchands = barres(marchands.map(r => str(r, "merchant_name") -> num(r, "revenue")), euros, teinte = false)

      // ---- rétention
      val badgeCohorte = meilleure.headOption.map { r =>
        "M+" + horizon + " : " + str(r, "cohort_month") + ", " + precise.format(num(r, "retention_percent")) + " %"
      }.getOrElse("")

      // ---- RFM
      val colonnesRfm = rfm.headOption.map(_.schema.fieldNames.filterNot(_ == "rfm_segment").toSeq).getOrElse(Seq.empty)
      val tableRfm = table("Segment RFM" +: colonnesRfm,
        rfm.map(r => str(r, "rfm_segment") +: colonnesRfm.map(cn => entier(num(r, cn)))),
        colonnesRfm.indices.map(_ + 1).toSet)
      val barresRfm = barres(rfm.map(r => str(r, "rfm_segment") -> colonnesRfm.map(cn => num(r, cn)).sum), entier, teinte = true)

      // ---- produits et paiements
      val tableProduits = table(
        Seq("Produit", "Chiffre d'affaires", "Note", "Stock"),
        produits.map { r =>
          // Le catalogue contient des noms manquants : l'identifiant prend le
          // relais plutôt que de laisser une cellule vide dans le tableau.
          val nom = Some(str(r, "product_name")).filter(_.trim.nonEmpty).getOrElse(str(r, "product_id"))
          Seq(nom, euros(num(r, "revenue")), precise.format(num(r, "average_rating")), entier(num(r, "stock")))
        },
        Set(1, 2, 3))
      // Une modalité vide correspond à une valeur manquante : elle n'a pas sa
      // place dans une légende, son volume reste visible dans les rapports CSV.
      def regrouper(rows: Seq[Row], cle: String): Seq[(String, Double)] =
        rows.groupBy(r => str(r, cle)).toSeq
          .filter { case (k, _) => k.trim.nonEmpty }
          .map { case (k, lignes) => k -> lignes.map(r => num(r, "revenue")).sum }
          .sortBy(-_._2)
      val parPaiement = regrouper(paiements, "payment_method")
      val parPeriode = regrouper(paiements, "day_period")
      val parCategorie = regrouper(categories, "product_category").take(10)

      // ---- exécution
      val barresTimings = barres(
        timings.filterNot(_.etape == "total").map(t => t.etape -> t.duree_ms / 1000.0),
        d => precise.format(d) + " s", teinte = true)

      val contenu =
        Seq(
          s"""<div id="apercu">$enTete</div>""",
          s"""<div class="grid">
             |${carte("qualite", "Qualité des données", "", entonnoir + tableQualite + orphelins, "col-8")}
             |${carte("", "Répartition par moyen de paiement", "",
                  anneau(parPaiement, compact(revenu), "EUR au total"), "col-4")}
             |</div>""".stripMargin,
          s"""<div class="grid">
             |${carte("marchands", "Premiers marchands", "Chiffre d'affaires", barresMarchands, "col-5")}
             |${carte("", "Détail par marchand", "", tableMarchands, "col-7")}
             |</div>""".stripMargin,
          s"""<div class="grid">
             |${carte("retention", "Matrice de rétention par cohorte", badgeCohorte, heatmap(cohortes, colonnes), "col-12")}
             |</div>""".stripMargin,
          s"""<div class="grid">
             |${carte("rfm", "Clients par segment RFM", "", barresRfm, "col-5")}
             |${carte("", "Segment RFM croisé au segment déclaré", "", tableRfm, "col-7")}
             |</div>""".stripMargin,
          s"""<div class="grid">
             |${carte("produits", "Meilleurs produits", "", tableProduits, "col-6")}
             |${carte("", "Chiffre d'affaires par catégorie", "", barres(parCategorie, euros, teinte = false), "col-6")}
             |</div>""".stripMargin,
          s"""<div class="grid">
             |${carte("", "Chiffre d'affaires par période de la journée", "",
                  anneau(parPeriode, entier(transactions), "transactions"), "col-4")}
             |${carte("execution", "Durée par étape", precise.format(total) + " s au total", barresTimings, "col-8")}
             |</div>""".stripMargin
        ).mkString("\n")

      val html = page(contenu, horodatage, transactions, debut, fin)
      val chemin = new Path(s"$out/dashboard.html")
      val fs = chemin.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val flux = fs.create(chemin, true)
      try flux.write(html.getBytes(StandardCharsets.UTF_8)) finally flux.close()
      println(s"TABLEAU DE BORD : $out/dashboard.html")
    } catch {
      // La restitution est un confort : son échec ne doit jamais casser le pipeline.
      case NonFatal(e) => System.err.println("Tableau de bord non généré : " + e.getMessage)
    }
  }

  private def navigation: String = {
    val entrees = Seq(
      ("apercu", "Vue d'ensemble"), ("qualite", "Qualité"), ("marchands", "Marchands"),
      ("retention", "Rétention"), ("rfm", "Segments RFM"), ("produits", "Produits"), ("execution", "Exécution"))
    entrees.zipWithIndex.map { case ((id, libelle), i) =>
      s"""<a href="#$id"${if (i == 0) " class='actif'" else ""}>${escape(libelle)}</a>"""
    }.mkString("\n")
  }

  private def page(contenu: String, horodatage: String,
                   transactions: Double, debut: String, fin: String): String =
    s"""<!doctype html>
       |<html lang="fr">
       |<head>
       |<meta charset="utf-8">
       |<meta name="viewport" content="width=device-width, initial-scale=1">
       |<title>EcommerceAnalytics</title>
       |<style>$feuilleDeStyle</style>
       |</head>
       |<body>
       |<div class="app">
       |  <aside class="sidebar">
       |    <div class="marque">
       |      <span class="logo">EA</span>
       |      <span class="marque-nom">EcommerceAnalytics</span>
       |    </div>
       |    <nav>$navigation</nav>
       |    <div class="sidebar-pied">
       |      <p class="moteur">Apache Spark 3.5.6</p>
       |      <p class="moteur">Scala 2.12 &middot; Java 17</p>
       |    </div>
       |  </aside>
       |  <div class="principal">
       |    <header class="topbar">
       |      <div>
       |        <h1>Analytique e commerce</h1>
       |        <p class="fenetre">${escape(debut)} &rarr; ${escape(fin)} &middot; ${escape(entier(transactions))} transactions</p>
       |      </div>
       |      <div class="outils">
       |        <span class="horodatage">${escape(horodatage)}</span>
       |        <button type="button" id="theme" class="bascule" aria-label="Changer de thème">
       |          <span class="soleil">&#9788;</span><span class="lune">&#9789;</span>
       |        </button>
       |      </div>
       |    </header>
       |    <main>
       |$contenu
       |    </main>
       |  </div>
       |</div>
       |<script>$script</script>
       |</body>
       |</html>""".stripMargin

  private val script: String =
    """
      |(function () {
      |  var racine = document.documentElement;
      |  var cle = 'ea-theme';
      |  function appliquer(t) { racine.setAttribute('data-theme', t); }
      |  var enregistre = null;
      |  try { enregistre = localStorage.getItem(cle); } catch (e) { enregistre = null; }
      |  if (enregistre === 'clair' || enregistre === 'sombre') {
      |    appliquer(enregistre);
      |  } else {
      |    var sombre = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
      |    appliquer(sombre ? 'sombre' : 'clair');
      |  }
      |  var bouton = document.getElementById('theme');
      |  if (bouton) {
      |    bouton.addEventListener('click', function () {
      |      var suivant = racine.getAttribute('data-theme') === 'sombre' ? 'clair' : 'sombre';
      |      appliquer(suivant);
      |      try { localStorage.setItem(cle, suivant); } catch (e) { /* stockage indisponible */ }
      |    });
      |  }
      |  var liens = Array.prototype.slice.call(document.querySelectorAll('.sidebar nav a'));
      |  var cibles = liens.map(function (a) { return document.querySelector(a.getAttribute('href')); });
      |  function surligner() {
      |    var haut = window.scrollY + 140, actif = 0;
      |    cibles.forEach(function (el, i) { if (el && el.offsetTop <= haut) { actif = i; } });
      |    liens.forEach(function (a, i) { a.classList.toggle('actif', i === actif); });
      |  }
      |  window.addEventListener('scroll', surligner, { passive: true });
      |  surligner();
      |}());
      |""".stripMargin

  private val feuilleDeStyle: String =
    """
      |:root{
      |  --bleu:#3b82f6; --violet:#8b5cf6; --ambre:#f5b301; --vert:#10b981;
      |  --rose:#ec4899; --cyan:#06b6d4;
      |  --sans:'Inter','Segoe UI',system-ui,-apple-system,sans-serif;
      |  --mono:'JetBrains Mono','Cascadia Mono',Consolas,monospace;
      |  --rayon:16px;
      |}
      |:root[data-theme="clair"]{
      |  --fond:#f4f5f9; --carte:#ffffff; --barre:#ffffff;
      |  --encre:#14161f; --doux:#6b7280; --tres-doux:#9aa1ae;
      |  --trait:#e8eaf0; --piste:#eef0f6; --survol:#f7f8fc;
      |  --ombre:0 1px 2px rgba(16,20,40,.04), 0 8px 24px rgba(16,20,40,.06);
      |}
      |:root[data-theme="sombre"]{
      |  --fond:#0f0f17; --carte:#1a1a26; --barre:#161620;
      |  --encre:#f3f4f8; --doux:#9ca3b4; --tres-doux:#6f7688;
      |  --trait:#272736; --piste:#23232f; --survol:#20202d;
      |  --ombre:0 1px 2px rgba(0,0,0,.4);
      |}
      |*{box-sizing:border-box}
      |html,body{margin:0;padding:0}
      |body{background:var(--fond);color:var(--encre);font-family:var(--sans);
      |     font-size:14px;line-height:1.55;-webkit-font-smoothing:antialiased}
      |a{color:inherit;text-decoration:none}
      |
      |.app{display:flex;min-height:100vh}
      |
      |/* ---------------------------------------------------------- sidebar */
      |.sidebar{width:236px;flex:0 0 236px;background:var(--barre);border-right:1px solid var(--trait);
      |         padding:22px 16px;display:flex;flex-direction:column;gap:26px;
      |         position:sticky;top:0;height:100vh}
      |.marque{display:flex;align-items:center;gap:11px;padding:0 8px}
      |.logo{width:34px;height:34px;border-radius:10px;background:var(--bleu);color:#fff;
      |      display:grid;place-items:center;font-weight:700;font-size:13px;letter-spacing:.5px}
      |.marque-nom{font-weight:650;font-size:14.5px;letter-spacing:-.01em}
      |.sidebar nav{display:flex;flex-direction:column;gap:2px}
      |.sidebar nav a{padding:9px 12px;border-radius:10px;color:var(--doux);font-size:13.5px;font-weight:500}
      |.sidebar nav a:hover{background:var(--survol);color:var(--encre)}
      |.sidebar nav a.actif{background:var(--bleu);color:#fff}
      |.sidebar-pied{margin-top:auto;padding:0 12px}
      |.moteur{margin:0;font-size:11px;color:var(--tres-doux);font-family:var(--mono)}
      |
      |/* ---------------------------------------------------------- topbar */
      |.principal{flex:1;min-width:0;display:flex;flex-direction:column}
      |.topbar{display:flex;align-items:center;justify-content:space-between;gap:24px;
      |        padding:20px 28px;border-bottom:1px solid var(--trait);background:var(--barre);
      |        position:sticky;top:0;z-index:5}
      |.topbar h1{margin:0;font-size:19px;font-weight:650;letter-spacing:-.015em}
      |.fenetre{margin:2px 0 0;font-size:12.5px;color:var(--doux)}
      |.outils{display:flex;align-items:center;gap:14px}
      |.horodatage{font-family:var(--mono);font-size:11.5px;color:var(--tres-doux);
      |            padding:6px 11px;border:1px solid var(--trait);border-radius:8px}
      |.bascule{width:56px;height:30px;border-radius:999px;border:1px solid var(--trait);
      |         background:var(--piste);cursor:pointer;display:flex;align-items:center;
      |         justify-content:space-around;font-size:13px;color:var(--doux);padding:0 6px}
      |.bascule:hover{border-color:var(--bleu)}
      |:root[data-theme="clair"] .bascule .soleil{color:var(--ambre)}
      |:root[data-theme="sombre"] .bascule .lune{color:var(--bleu)}
      |:root[data-theme="clair"] .bascule .lune,
      |:root[data-theme="sombre"] .bascule .soleil{opacity:.32}
      |
      |/* ---------------------------------------------------------- grille */
      |main{padding:24px 28px 56px;display:flex;flex-direction:column;gap:18px}
      |.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:18px}
      |.col-4{grid-column:span 4}.col-5{grid-column:span 5}.col-6{grid-column:span 6}
      |.col-7{grid-column:span 7}.col-8{grid-column:span 8}.col-12{grid-column:span 12}
      |.kpis{grid-template-columns:repeat(6,1fr)}
      |
      |.card{background:var(--carte);border:1px solid var(--trait);border-radius:var(--rayon);
      |      padding:20px;box-shadow:var(--ombre);min-width:0}
      |.card-head{display:flex;align-items:center;justify-content:space-between;gap:14px;margin:0 0 14px}
      |.card-head h2{margin:0;font-size:14.5px;font-weight:650;letter-spacing:-.01em}
      |.badge{font-size:11.5px;font-weight:600;color:var(--bleu);background:var(--piste);
      |       padding:4px 10px;border-radius:999px;white-space:nowrap}
      |
      |.kpi{padding:18px}
      |.kpi-label{margin:0;font-size:11.5px;color:var(--doux);font-weight:500}
      |.kpi-value{margin:7px 0 0;font-size:25px;font-weight:700;letter-spacing:-.025em;
      |           font-variant-numeric:tabular-nums;line-height:1.15}
      |.chip{margin:10px 0 0;font-size:11px;color:var(--c);font-weight:600;
      |      display:inline-block;padding:3px 9px;border-radius:999px;
      |      background:color-mix(in srgb, var(--c) 14%, transparent)}
      |
      |/* ---------------------------------------------------------- tables */
      |.scroll{overflow-x:auto;margin:0 -4px}
      |table{width:100%;border-collapse:collapse;font-size:13px}
      |thead th{text-align:left;padding:0 12px 9px;font-size:11px;color:var(--doux);
      |         font-weight:600;border-bottom:1px solid var(--trait);white-space:nowrap}
      |tbody td{padding:10px 12px;border-bottom:1px solid var(--trait);white-space:nowrap}
      |tbody tr:last-child td{border-bottom:none}
      |tbody tr:hover td{background:var(--survol)}
      |.num{text-align:right;font-variant-numeric:tabular-nums}
      |
      |/* ---------------------------------------------------------- barres */
      |.bars{list-style:none;margin:0;padding:0}
      |.bars li{display:grid;grid-template-columns:minmax(96px,1.1fr) 2.2fr minmax(88px,auto);
      |         gap:14px;align-items:center;padding:7px 0}
      |.bar-name{font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      |.bar-track{height:8px;background:var(--piste);border-radius:999px;overflow:hidden}
      |.bar-fill{display:block;height:100%;border-radius:999px}
      |.bar-value{font-size:12px;text-align:right;font-variant-numeric:tabular-nums;color:var(--doux)}
      |
      |.funnel{list-style:none;margin:0 0 18px;padding:0}
      |.funnel li{display:grid;grid-template-columns:88px 1fr 66px;gap:14px;align-items:center;padding:5px 0}
      |.fun-name{font-size:13px;color:var(--doux)}
      |.fun-track{height:30px;background:var(--piste);border-radius:8px;overflow:hidden}
      |.fun-fill{display:flex;align-items:center;justify-content:flex-end;height:100%;
      |          padding-right:11px;color:#fff;font-size:12px;font-weight:600;
      |          font-variant-numeric:tabular-nums;border-radius:8px}
      |.fun-value{text-align:right;font-size:12px;color:var(--doux);font-variant-numeric:tabular-nums}
      |
      |.mini-stats{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:16px}
      |.mini-stats div{background:var(--piste);border-radius:12px;padding:13px 15px}
      |.mini-stats strong{display:block;font-size:19px;font-weight:700;font-variant-numeric:tabular-nums}
      |.mini-stats span{font-size:11.5px;color:var(--doux);font-family:var(--mono)}
      |
      |/* ---------------------------------------------------------- anneau */
      |.anneau{display:flex;flex-direction:column;align-items:center;gap:18px}
      |.anneau-svg{position:relative;width:140px;height:140px}
      |.anneau-svg svg{transform:rotate(-90deg)}
      |.anneau-centre{position:absolute;inset:0;display:flex;flex-direction:column;
      |               align-items:center;justify-content:center;gap:2px}
      |.anneau-centre strong{font-size:18px;font-weight:700;letter-spacing:-.02em}
      |.anneau-centre span{font-size:10.5px;color:var(--doux)}
      |.legende{list-style:none;margin:0;padding:0;width:100%}
      |.legende li{display:grid;grid-template-columns:10px 1fr auto;gap:10px;align-items:center;padding:5px 0}
      |.puce{width:10px;height:10px;border-radius:3px}
      |.leg-nom{font-size:12.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      |.leg-val{font-size:12px;color:var(--doux);font-variant-numeric:tabular-nums}
      |
      |/* ---------------------------------------------------------- heatmap */
      |table.heat td.cell{text-align:right;font-variant-numeric:tabular-nums;font-size:12px;
      |                   border-bottom:2px solid var(--carte);border-right:2px solid var(--carte)}
      |table.heat td.cell.fonce{color:#fff}
      |table.heat td.neutre{background:var(--piste)}
      |table.heat th.cohorte{font-weight:600;font-size:12.5px;white-space:nowrap;padding:10px 12px}
      |table.heat td.taille{color:var(--doux)}
      |table.heat tbody tr:hover td{background:inherit}
      |.vide{color:var(--doux)}
      |
      |/* ---------------------------------------------------------- adaptatif */
      |@media (max-width:1280px){
      |  .kpis{grid-template-columns:repeat(3,1fr)}
      |  .col-4,.col-5,.col-6,.col-7,.col-8{grid-column:span 12}
      |}
      |@media (max-width:860px){
      |  .sidebar{display:none}
      |  .kpis{grid-template-columns:repeat(2,1fr)}
      |  main{padding:18px}
      |  .topbar{padding:16px 18px}
      |}
      |@media print{
      |  .sidebar,.outils{display:none}
      |  .card{break-inside:avoid;box-shadow:none}
      |}
      |""".stripMargin
}
