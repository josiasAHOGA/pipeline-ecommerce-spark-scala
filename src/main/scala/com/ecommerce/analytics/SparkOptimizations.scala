package com.ecommerce.analytics

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer

/**
 * Réglages de performance du pipeline, regroupés en un seul endroit.
 *
 * Trois leviers y sont pilotés : la mise en cache des jeux réutilisés,
 * l'exécution adaptative du plan Spark, et la libération explicite de la
 * mémoire. La diffusion des référentiels par `broadcast` est déclenchée dans
 * les jointures elles mêmes, mais reste commandée par la même configuration.
 *
 * Le drapeau `enabled` neutralise l'ensemble du bloc : c'est ce qui permet au
 * mode `benchmark` de rejouer le pipeline sans aucune optimisation, puis avec,
 * et de comparer deux exécutions réellement différentes.
 *
 * Chaque clé est lue avec une valeur de repli, de sorte que le programme
 * fonctionne même si le fichier de configuration ne la déclare pas.
 */
class SparkOptimizations(c: Config, enabled: Boolean) {

  private val retained = ArrayBuffer.empty[DataFrame]

  private def bool(chemin: String, defaut: Boolean): Boolean =
    if (c.hasPath(chemin)) c.getBoolean(chemin) else defaut

  /**
   * Applique à la session les réglages qui relèvent de la performance et non
   * de la construction de la session.
   *
   * L'exécution adaptative réoptimise le plan pendant l'exécution, à partir
   * des statistiques réelles des échanges : elle fusionne les partitions de
   * shuffle devenues trop petites et découpe celles qui sont anormalement
   * grosses. Sur ce jeu de données elle joue surtout sur l'étape analytique,
   * qui enchaîne agrégations et fenêtres.
   *
   * Ces trois clés sont modifiables après la création de la session, ce qui
   * n'est pas le cas du sérialiseur : `spark.serializer` est un paramètre
   * statique. Nous restons donc sur le sérialiseur par défaut, sans
   * conséquence ici puisque toutes nos opérations passent par l'API DataFrame,
   * dont le moteur Tungsten gère lui même un format binaire compact.
   */
  def appliquer(spark: SparkSession): Unit = {
    val adaptatif = enabled && bool("app.optimization.adaptive.enabled", true)
    spark.conf.set("spark.sql.adaptive.enabled", adaptatif)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",
      adaptatif && bool("app.optimization.adaptive.coalesce-partitions", true))
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",
      adaptatif && bool("app.optimization.adaptive.skew-join", true))

    val cacheActif = enabled && bool("app.optimization.enable-cache", true)
    val diffusion = enabled && bool("app.optimization.enable-broadcast", true)
    println(s"OPTIMISATIONS : cache=$cacheActif, broadcast=$diffusion, execution adaptative=$adaptatif")
  }

  /**
   * Conserve un DataFrame qui sera relu plus loin dans le pipeline.
   *
   * Un jeu volumineux est sérialisé et peut déborder sur disque, ce qui évite
   * de le recalculer entièrement quand la mémoire manque. Un petit rapport
   * reste en mémoire décompressée, plus rapide à relire.
   *
   * Mettre en cache un DataFrame utilisé une seule fois coûte sans rien
   * rapporter : seuls les jeux réellement relus passent par cette méthode.
   */
  def keep(df: DataFrame, large: Boolean = false): DataFrame = {
    if (enabled && bool("app.optimization.enable-cache", true)) {
      if (large) df.persist(StorageLevel.MEMORY_AND_DISK_SER) else df.cache()
      retained += df
    }
    df
  }

  /** Nombre de jeux actuellement retenus en mémoire. */
  def retenus: Int = retained.size

  /**
   * Libère les jeux retenus, du plus récent au plus ancien.
   *
   * L'ordre inverse évite de libérer un jeu dont un autre, encore en cache,
   * dépend. L'appel est fait dans un bloc `finally` : même un échec du
   * pipeline ne laisse pas de blocs occupés.
   */
  def release(): Unit = retained.reverse.foreach(_.unpersist(blocking = true))
}
