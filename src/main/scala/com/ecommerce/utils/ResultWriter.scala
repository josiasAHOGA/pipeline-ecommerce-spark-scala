package com.ecommerce.utils
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType,MapType,StructType}
class ResultWriter(c: Config, root: String) {
  def write(name: String, df: DataFrame): Unit = {
    val p = c.getInt("app.data.output.partitions")
    val mode = c.getString("app.data.output.mode")
    val flat = df.select(df.schema.fields.map { f => f.dataType match {
      case _: ArrayType | _: MapType | _: StructType => to_json(col(f.name)).as(f.name)
      case _ => col(f.name)
    }}: _*)
    flat.coalesce(p).write.mode(mode).option("header",true).option("encoding","UTF-8")
      .option("escape", "\"").csv(s"$root/csv/$name")
    df.coalesce(p).write.mode(mode).parquet(s"$root/parquet/$name")
  }
}
