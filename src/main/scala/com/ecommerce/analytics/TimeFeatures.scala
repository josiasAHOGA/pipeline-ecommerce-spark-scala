package com.ecommerce.analytics
import java.time.{LocalDateTime, DayOfWeek}
import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.util.Locale
import scala.util.Try
import com.ecommerce.models.TimeInfo
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TimeFeatures {
  def parse(value: String, pattern: String): Option[LocalDateTime] = Option(value).filter(_.nonEmpty)
    .flatMap(s => Try(LocalDateTime.parse(s,DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.STRICT))).toOption)
  def extractTimeFeatures(c: Config): UserDefinedFunction = {
    val pattern = c.getString("app.validation.transaction.timestamp-pattern")
    val morning = c.getInt("app.time.morning-start"); val afternoon = c.getInt("app.time.afternoon-start")
    val evening = c.getInt("app.time.evening-start"); val night = c.getInt("app.time.night-start")
    val start = c.getInt("app.time.working-start"); val end = c.getInt("app.time.working-end-inclusive")
    udf((s: String) => parse(s, pattern).map { dt =>
      val h = dt.getHour
      TimeInfo(h,dt.getDayOfWeek.getDisplayName(TextStyle.FULL,Locale.FRENCH),
        dt.getMonth.getDisplayName(TextStyle.FULL,Locale.FRENCH),
        if(Set(DayOfWeek.SATURDAY,DayOfWeek.SUNDAY)(dt.getDayOfWeek)) 1 else 0,
        if(h < morning || h >= night) "Night" else if(h < afternoon) "Morning" else if(h < evening) "Afternoon" else "Evening",
        if(h >= start && h <= end) 1 else 0)
    }.orNull)
  }
}
