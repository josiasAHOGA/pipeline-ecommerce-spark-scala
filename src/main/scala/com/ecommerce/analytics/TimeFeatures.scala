package com.ecommerce.analytics

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.util.Locale

import scala.util.Try

import com.ecommerce.models.TimeInfo
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * Question 3.1 du sujet : UDF `extractTimeFeatures`.
 *
 * Une seule chaîne `yyyyMMddHHmmss` produit six caractéristiques temporelles
 * réutilisées ensuite par tout le pipeline : l'heure, le jour de la semaine et
 * le mois en clair, un drapeau week end, une étiquette de période de journée et
 * un drapeau d'heures ouvrées.
 *
 * Trois décisions structurent ce fichier.
 *
 *  1. La logique métier vit dans des fonctions pures (`dayPeriod`, `isWeekend`,
 *     `isWorkingHours`, `features`), et l'UDF n'est qu'une enveloppe Spark. Ces
 *     fonctions se testent sans SparkSession, et surtout `DataValidation`
 *     réutilise `parse` sans dépendre de l'UDF.
 *  2. Le parsing est strict et total. Une entrée nulle, vide ou impossible
 *     renvoie `None`, jamais une exception : une UDF qui lève une exception fait
 *     échouer la tâche Spark entière, donc tout le job, pour une seule ligne
 *     sale. Or le sujet garantit la présence d'horodatages mal formés.
 *  3. Les bornes horaires ne sont pas écrites dans le code. Elles sont lues une
 *     fois dans la configuration (Partie 7) et transportées par `TimeSettings`,
 *     un objet sérialisable. `Config` ne l'est pas : le capturer dans la
 *     fermeture de l'UDF provoquerait une `NotSerializableException` au moment
 *     de l'envoi de la tâche aux exécuteurs.
 */
object TimeFeatures {

  /**
   * Paramètres temporels résolus, sérialisables, transportés vers les
   * exécuteurs Spark. Toutes les valeurs viennent de la configuration.
   */
  case class TimeSettings(
      pattern: String,
      morningStart: Int,
      afternoonStart: Int,
      eveningStart: Int,
      nightStart: Int,
      workingStart: Int,
      workingEndInclusive: Int,
      locale: Locale)

  /** Langue de repli des libellés si `app.time.locale` est absent du fichier. */
  private val LangueParDefaut = "fr"

  private val Weekend: Set[DayOfWeek] = Set(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)

  /** Lit les bornes une seule fois, côté pilote. */
  def settings(c: Config): TimeSettings = TimeSettings(
    pattern = c.getString("app.validation.transaction.timestamp-pattern"),
    morningStart = c.getInt("app.time.morning-start"),
    afternoonStart = c.getInt("app.time.afternoon-start"),
    eveningStart = c.getInt("app.time.evening-start"),
    nightStart = c.getInt("app.time.night-start"),
    workingStart = c.getInt("app.time.working-start"),
    workingEndInclusive = c.getInt("app.time.working-end-inclusive"),
    // Mécanisme de valeur par défaut demandé par la Question 7.1 : une clé
    // absente ne doit pas faire échouer le chargement.
    locale = Locale.forLanguageTag(
      if (c.hasPath("app.time.locale")) c.getString("app.time.locale") else LangueParDefaut))

  /**
   * Convertit `yyyyMMddHHmmss` en date, ou `None` si la chaîne est nulle, vide
   * ou n'est pas une date réelle.
   *
   * `ResolverStyle.STRICT` est indispensable : en mode indulgent, le
   * 29 février 2025, qui n'existe pas, serait silencieusement décalé au
   * 1er mars et une donnée fausse entrerait dans le pipeline. Le motif utilise
   * `uuuu` et non `yyyy`, car le mode strict exige une année sans ambiguïté
   * d'ère.
   */
  def parse(value: String, pattern: String): Option[LocalDateTime] =
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap { texte =>
        val format = DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.STRICT)
        Try(LocalDateTime.parse(texte, format)).toOption
      }

  /**
   * Étiquette de période de journée.
   *
   * Le sujet définit Morning [6h, 12h[, Afternoon [12h, 18h[, Evening
   * [18h, 22h[ et Night à partir de 22h, mais ne dit rien de la plage
   * [0h, 6h[. Nous la classons en Night, seule convention cohérente avec
   * l'idée de nuit, et cette frontière est testée explicitement.
   */
  def dayPeriod(hour: Int, s: TimeSettings): String =
    if (hour < s.morningStart || hour >= s.nightStart) "Night"
    else if (hour < s.afternoonStart) "Morning"
    else if (hour < s.eveningStart) "Afternoon"
    else "Evening"

  /** 1 le samedi et le dimanche, 0 les autres jours. */
  def isWeekend(day: DayOfWeek): Int = if (Weekend(day)) 1 else 0

  /**
   * 1 si l'heure entière est comprise entre les bornes ouvrées incluses.
   *
   * La borne haute est inclusive : avec 9 et 17, 17h59 reste une heure ouvrée
   * et 18h00 n'en est plus une. Le sujet parle d'une heure « comprise entre 9h
   * et 17h », donc du test sur l'heure entière et non sur la minute.
   */
  def isWorkingHours(hour: Int, s: TimeSettings): Int =
    if (hour >= s.workingStart && hour <= s.workingEndInclusive) 1 else 0

  /**
   * Libellé lisible dans un rapport : « samedi » devient « Samedi ».
   *
   * La majuscule est appliquée avec la locale cible, car certaines langues ont
   * des règles de casse spécifiques.
   */
  private def libelle(texte: String, locale: Locale): String =
    if (texte.isEmpty) texte else texte.substring(0, 1).toUpperCase(locale) + texte.substring(1)

  /**
   * Cœur métier de la Question 3.1, sans aucune dépendance à Spark : une chaîne
   * en entrée, une structure de six champs en sortie, ou `None` si la chaîne
   * n'est pas exploitable.
   */
  def features(value: String, s: TimeSettings): Option[TimeInfo] =
    parse(value, s.pattern).map { dt =>
      val heure = dt.getHour
      TimeInfo(
        hour = heure,
        day_of_week = libelle(dt.getDayOfWeek.getDisplayName(TextStyle.FULL, s.locale), s.locale),
        month = libelle(dt.getMonth.getDisplayName(TextStyle.FULL, s.locale), s.locale),
        is_weekend = isWeekend(dt.getDayOfWeek),
        day_period = dayPeriod(heure, s),
        is_working_hours = isWorkingHours(heure, s))
    }

  /**
   * UDF exposée à Spark. Renvoie `null` plutôt qu'une exception sur une entrée
   * invalide : la ligne fautive porte alors une structure nulle, visible dans
   * les sorties, et le job continue.
   */
  def extractTimeFeatures(c: Config): UserDefinedFunction = {
    val s = settings(c)
    udf((value: String) => features(value, s).orNull)
  }
}
