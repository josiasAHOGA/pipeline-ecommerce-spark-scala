package com.ecommerce.analytics
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
class SparkOptimizations(c: Config, enabled: Boolean) {
  private val retained = ArrayBuffer.empty[DataFrame]
  def keep(df: DataFrame, large: Boolean = false): DataFrame = {
    if(enabled && c.getBoolean("app.optimization.enable-cache")) {
      if(large) df.persist(StorageLevel.MEMORY_AND_DISK_SER) else df.cache()
      retained += df
    }
    df
  }
  def release(): Unit = retained.reverse.foreach(_.unpersist(blocking=true))
}
