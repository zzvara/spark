// scalastyle:off
package org.apache.spark.traceable.examples

import org.apache.spark.{SparkConf, SparkContext}

object Wordcount {
  def main(args: Array[String]): Unit = {
    val configuration = new SparkConf()
      .setMaster("local[10]")
      .setAppName("Traceable wordcount")

    val context = new SparkContext(configuration)

    context
      .textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach {
        println
      }
  }
}
