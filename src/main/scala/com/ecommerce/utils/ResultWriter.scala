package com.ecommerce.utils

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

/**
 * Écriture des rapports, en CSV pour la lecture humaine et en Parquet pour
 * conserver les types.
 *
 * Le nombre de fichiers produits n'est pas le même selon la taille du rapport.
 * Ramener un jeu à une seule partition concentre tout le travail d'écriture
 * sur une tâche unique, et, si le jeu n'a pas déjà été matérialisé, l'ensemble
 * des transformations en amont avec lui. Le coût est invisible sur un rapport
 * de quelques centaines de lignes, mais réel sur les transactions enrichies.
 *
 * Les rapports d'analyse restent donc en un fichier, ce qui les rend
 * directement consultables, tandis que les jeux volumineux conservent
 * plusieurs partitions écrites en parallèle.
 */
class ResultWriter(c: Config, root: String) {

  private def entier(chemin: String, defaut: Int): Int =
    if (c.hasPath(chemin)) c.getInt(chemin) else defaut

  /**
   * @param volumineux vrai pour les jeux à l'échelle des transactions, faux
   *                   pour les rapports agrégés.
   */
  def write(name: String, df: DataFrame, volumineux: Boolean = false): Unit = {
    val partitions =
      if (volumineux) entier("app.data.output.partitions-large", 8)
      else c.getInt("app.data.output.partitions")
    val mode = c.getString("app.data.output.mode")

    // Un CSV ne sait représenter ni un tableau ni une structure : ces colonnes
    // sont converties en JSON pour le seul export CSV. Le Parquet, lui, garde
    // les types d'origine.
    val flat = df.select(df.schema.fields.map { f =>
      f.dataType match {
        case _: ArrayType | _: MapType | _: StructType => to_json(col(f.name)).as(f.name)
        case _ => col(f.name)
      }
    }: _*)

    flat.coalesce(partitions).write.mode(mode)
      .option("header", true).option("encoding", "UTF-8").option("escape", "\"")
      .csv(s"$root/csv/$name")

    df.coalesce(partitions).write.mode(mode).parquet(s"$root/parquet/$name")
  }
}
