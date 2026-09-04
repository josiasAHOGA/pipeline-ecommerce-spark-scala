package com.ecommerce.utils
import com.typesafe.config.{Config, ConfigFactory}
object ConfigLoader {
  def load(): Config = ConfigFactory.load().resolve()
}
