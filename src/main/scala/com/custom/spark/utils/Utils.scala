package com.custom.spark.utils

object Utils {

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
  }

}
