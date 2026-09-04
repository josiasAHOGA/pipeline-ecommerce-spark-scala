package com.ecommerce.analytics
import java.time.Instant
import com.ecommerce.models.Timing
import com.ecommerce.report.DashboardReport
import com.ecommerce.utils._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object MainApp {
  val stages = Set("all","ingestion","transformation","analytics","benchmark")
  def main(args: Array[String]): Unit = {
    // Le pattern matching de Scala remplace ici une cascade de tests : un
    // argument inconnu ou surnumeraire tombe dans le cas par defaut, qui
    // affiche l'aide sans lever d'exception (Question 6.2 du sujet).
    args.toList match {
      case Nil => executer("all")
      case unique :: Nil if stages(unique) => executer(unique)
      case _ => println("Usage : MainApp [all|ingestion|transformation|analytics|benchmark]")
    }
  }

  private def executer(stage: String): Unit = {
    val c = ConfigLoader.load()
    var spark: SparkSession = null
    try {
      spark = SparkSessionBuilder.build(c)
      val activeSpark = spark
      import activeSpark.implicits._
      val sortie = c.getString("app.data.output.path")
      stage match {
        case "benchmark" =>
          val before = run(spark,c,"all",false,sortie+"/benchmark/sans")
          val after = run(spark,c,"all",true,sortie+"/benchmark/avec")
          val rows = (before++after).toDF
          val comparison = rows.groupBy("etape").pivot("mode",Seq("sans","avec")).agg(first("duree_ms"))
            .withColumn("gain_percent",round((col("sans")-col("avec"))/col("sans")*100,2))
          new ResultWriter(c,sortie).write("benchmark_comparison",comparison)
          comparison.show(false)
        case autre =>
          run(spark,c,autre,true,sortie)
      }
    } catch {
      case NonFatal(e) => System.err.println("ÉCHEC DU PIPELINE : "+e.getMessage); throw e
    } finally { if(spark != null) spark.stop() }
  }
  def run(spark: SparkSession,c: Config,stage: String,optimized: Boolean,out: String): Seq[Timing] = {
    import spark.implicits._
    val timings = ArrayBuffer.empty[Timing]
    val mode = if(optimized) "avec" else "sans"
    val cache = new SparkOptimizations(c,optimized)
    cache.appliquer(spark)
    val writer = new ResultWriter(c,out)
    // Jeux ecrits a l'echelle des transactions : ils gardent plusieurs
    // partitions, la concentration sur un seul fichier y couterait cher.
    val volumineux = Set("enriched_transactions","join_rejections")
    val startAll = System.nanoTime()
    def timed[A](name: String)(f: => A): A = {
      println(s"DÉBUT $name ${Instant.now()}")
      val start = System.nanoTime(); val result = f
      val ms = (System.nanoTime()-start)/1e6
      timings += Timing(mode,name,ms)
      println(f"FIN $name ${Instant.now()} $ms%.2f ms")
      result
    }
    try {
      val sources = timed("ingestion") { new DataIngestion(spark,c).loadAll() }
      val validation = new DataValidation(c)
      val v = timed("validation") {
        validation.validateAll(sources).map { case (n,r) =>
          n -> ValidationResult(cache.keep(r.valid),cache.keep(r.rejected))
        }
      }
      val quality = timed("qualité") { cache.keep(validation.quality(spark,sources,v)) }
      quality.show(false)
      val prepared = ArrayBuffer[(String,DataFrame)]("quality_report"->quality)
      v.toSeq.sortBy(_._1).foreach { case(n,r) => prepared += s"rejected_$n" -> r.rejected }
      if(stage!="ingestion") {
        val enriched = timed("transformation") {
          val e = new DataTransformation(c).enrichTransactionData(v,optimized)
          val saved = Enrichment(cache.keep(e.data,true),cache.keep(e.rejected))
          val n = saved.data.count(); val lost = saved.rejected.count()
          require(n+lost==v("transactions").valid.count(),"La jointure a dupliqué ou perdu des transactions")
          println(s"ENRICHISSEMENT : $n retenues, $lost rejetées")
          saved
        }
        prepared += "enriched_transactions" -> enriched.data
        prepared += "join_rejections" -> enriched.rejected
        if(stage!="transformation") {
          val reports = timed("analytique") {
            new Analytics(c).all(enriched.data).map { case(n,df) => val kept=cache.keep(df); kept.count(); n->kept }
          }
          prepared ++= reports
        }
      }
      timed("écriture") {
        prepared.foreach { case(name,df) =>
          println("RÉSULTAT : "+name)
          df.show(c.getInt("app.analytics.preview-rows"),false)
          writer.write(name,df,volumineux(name))
        }
      }
      timings += Timing(mode,"total",(System.nanoTime()-startAll)/1e6)
      writer.write("execution_timings",timings.toSeq.toDF)
      // Restitution décisionnelle : page HTML autonome alimentée par les sorties Gold.
      if(c.getBoolean("app.report.dashboard") && stage!="ingestion")
        new DashboardReport(c).render(spark,out,prepared.toSeq,timings.toSeq)
      timings.toSeq
    } finally { cache.release(); spark.catalog.clearCache() }
  }
}
